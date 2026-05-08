package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchManagerServiceGrpc;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Service responsible for proxying indexing requests to the OpenSearch Manager.
 * <p>
 * The sink no longer eagerly provisions indices — the admin paths
 * ({@code AssignSemanticConfigToIndex}, {@code BindVectorSetToIndex},
 * {@code ProvisionIndex}) are the single source of truth for index topology
 * and they must be called before any document is indexed. If the index/field
 * isn't pre-provisioned, the manager-side {@code IndexKnnProvisioner} fails
 * the doc loud with a clear pointer.
 * <p>
 * Sink → manager indexing uses the bidi streaming RPC
 * {@code StreamIndexDocuments} on a per-call basis: open a stream, send one
 * request, await one response, close the stream. Plain async-callback gRPC
 * stub on a virtual thread; no Mutiny on the hot path.
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);

    /** Per-call deadline on the bidi sink→manager round-trip. */
    private static final long INDEX_RPC_TIMEOUT_SECONDS = 30L;

    @Inject
    ChunkDocumentConverter chunkDocumentConverter;

    /**
     * Stork-discovered managed channel injected by Quarkus. Quarkus's
     * {@code @GrpcClient} only injects Mutiny stubs, blocking stubs, or
     * channels — so we take the channel and build a plain async-callback
     * stub ourselves at startup.
     */
    @Inject
    @GrpcClient("opensearch-manager")
    Channel managerChannel;

    /**
     * Plain async-callback stub for the bidi {@code StreamIndexDocuments} call.
     * Built from {@link #managerChannel} in {@link #init()}; package-private
     * so tests can substitute a fake.
     */
    OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub managerStub;

    @PostConstruct
    void init() {
        if (managerStub == null && managerChannel != null) {
            managerStub = OpenSearchManagerServiceGrpc.newStub(managerChannel);
        }
    }

    /**
     * Determine the index name for a document type on the experimental
     * streaming ingestion path. This is the only path that still needs a
     * "type → name" mapping; the production {@code processData} path routes
     * via {@link ResolvedPlan#indexName()} from the IndexPlan registry and
     * does not call this method.
     */
    public String determineIndexName(String documentType) {
        if (documentType == null || documentType.isEmpty()) {
            return "pipeline-documents";
        }
        return "pipeline-" + documentType.toLowerCase();
    }

    /**
     * Build a stable-ish provisioning cache key. Retained because tests still
     * use it to assert per-(index, semantic-config-set) bookkeeping in the
     * streaming path; the sink itself no longer caches provisioning state.
     */
    public String provisioningCacheKey(String indexName, PipeDoc document) {
        if (!document.hasSearchMetadata()) {
            return null;
        }
        return provisioningCacheKey(indexName, semanticConfigIds(document));
    }

    private Set<String> semanticConfigIds(PipeDoc document) {
        return document.getSearchMetadata().getSemanticResultsList().stream()
                .map(ai.pipestream.data.v1.SemanticProcessingResult::getSemanticConfigId)
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toCollection(HashSet::new));
    }

    private String provisioningCacheKey(String indexName, Set<String> semanticConfigIds) {
        return indexName + "|" + semanticConfigIds.stream().sorted().collect(Collectors.joining(","));
    }

    /**
     * Provisioning hook retained for the streaming path. Default implementation
     * is a no-op; tests override this to assert per-(index, configs)
     * provisioning bookkeeping or to inject failures.
     */
    public void ensureIndexProvisioned(String indexName, PipeDoc document, String documentType) {
        // no-op in production; admin paths own provisioning
    }

    /**
     * Index a single document via the OpenSearch Manager, scoped to a single
     * {@link ResolvedPlan}. The plan dictates the physical index name and the
     * indexing strategy used to fan documents out into chunk / vector indices;
     * the sink no longer carries either field on its config.
     */
    public String indexDocumentViaManager(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc,
                                          OpenSearchSinkOptions options) {
        return indexDocumentViaManager(plan, document, osDoc, options, "");
    }

    /**
     * Index a single document via the OpenSearch Manager, stamping the given
     * {@code crawlId} onto the indexed base document and every chunk so that
     * downstream per-run progress queries (GetCrawlIndexStats) can filter by
     * it. Empty crawlId is treated as "no stamp" — the field is left unset
     * and the docs index without a crawl_id (legacy behavior).
     * <p>
     * The {@code plan} carries the index name and indexing strategy. Routing
     * lives entirely in the plan; the sink's only contribution is the doc
     * payload + crawl_id stamping. {@code options} is currently unused inside
     * this method (BatchOptions belong to the streaming path) and is carried
     * only for forward-compatibility with future per-call knobs.
     * <p>
     * Wire-level call is the bidi {@code StreamIndexDocuments} RPC: open the
     * stream, send one request, await one response, close the stream.
     */
    @SuppressWarnings("unused")
    public String indexDocumentViaManager(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc,
                                          OpenSearchSinkOptions options, String crawlId) {
        String indexName = plan.indexName();
        // Stamp crawl_id on the base document up-front. The conversion path
        // (DocumentConverterService) sees only PipeDoc and can't know about
        // PipeStream metadata; the sink injects it here at the boundary.
        final OpenSearchDocument stampedOsDoc = (crawlId == null || crawlId.isEmpty())
                ? osDoc
                : osDoc.toBuilder().setCrawlId(crawlId).build();

        StreamIndexDocumentsRequest.Builder requestBuilder = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setIndexName(indexName)
                .setDocument(stampedOsDoc)
                .setDocumentId(document.getDocId());

        if (document.hasOwnership()) {
            requestBuilder.setAccountId(document.getOwnership().getAccountId());
            requestBuilder.setDatasourceId(document.getOwnership().getDatasourceId());
        }

        ai.pipestream.opensearch.v1.IndexingStrategy protoStrategy = plan.strategy();
        if (protoStrategy == null
                || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED) {
            // A READY plan must carry a concrete strategy. Coercing UNSPECIFIED
            // here would silently misroute documents to the wrong index shape;
            // refuse loud so the operator sees the misconfigured plan id.
            throw new IllegalStateException(
                    "IndexPlan '" + plan.id() + "' returned UNSPECIFIED indexing_strategy on a READY plan — "
                            + "manager contract violation; plan is misconfigured");
        }
        requestBuilder.setIndexingStrategy(protoStrategy);

        if (protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED
                || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES) {

            ChunkConversionResult chunkResult = chunkDocumentConverter.convertToChunks(
                    document, stampedOsDoc, indexName, protoStrategy);

            requestBuilder.setDocumentMap(chunkResult.documentMap());
            // Stamp crawl_id on every chunk so per-run progress queries
            // hit the side indices the same way they hit the base.
            if (crawlId != null && !crawlId.isEmpty()) {
                for (var chunk : chunkResult.chunkDocuments()) {
                    requestBuilder.addChunkDocuments(chunk.toBuilder().setCrawlId(crawlId).build());
                }
            } else {
                requestBuilder.addAllChunkDocuments(chunkResult.chunkDocuments());
            }

            LOG.infof("Enriched StreamIndexDocumentsRequest with %d chunk documents for strategy %s (doc %s, plan %s)",
                    chunkResult.chunkDocuments().size(), protoStrategy, document.getDocId(), plan.id());
        }

        StreamIndexDocumentsResponse response = streamOne(requestBuilder.build());
        if (response.getSuccess()) {
            return response.getMessage();
        }
        throw new RuntimeException(response.getMessage());
    }

    /**
     * Stream a micro-batch of indexing requests to the manager via one bidi
     * call: send every request, half-close, drain every response in order.
     * The caller is expected to be on a virtual thread; this method blocks
     * until the manager closes the stream or the per-call deadline expires.
     * <p>
     * Used by the experimental streaming ingestion path
     * ({@code OpenSearchIngestionServiceImpl.streamDocuments}). Tests override
     * this hook to record batch sizes without spinning up a real manager.
     */
    public java.util.List<StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
            java.util.List<StreamIndexDocumentsRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return java.util.List.of();
        }
        java.util.concurrent.BlockingQueue<Object> responses = new java.util.concurrent.LinkedBlockingQueue<>();
        Object COMPLETE = new Object();
        StreamObserver<StreamIndexDocumentsResponse> respObs = new StreamObserver<>() {
            @Override public void onNext(StreamIndexDocumentsResponse v) { responses.add(v); }
            @Override public void onError(Throwable t) { responses.add(t); }
            @Override public void onCompleted() { responses.add(COMPLETE); }
        };
        StreamObserver<StreamIndexDocumentsRequest> reqObs = managerStub.streamIndexDocuments(respObs);
        try {
            for (StreamIndexDocumentsRequest req : requests) {
                reqObs.onNext(req);
            }
            reqObs.onCompleted();
        } catch (RuntimeException e) {
            try {
                reqObs.onError(e);
            } catch (RuntimeException ignored) {
                // already failed; surface the original
            }
            throw e;
        }
        java.util.List<StreamIndexDocumentsResponse> collected = new java.util.ArrayList<>(requests.size());
        long deadlineNanos = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(INDEX_RPC_TIMEOUT_SECONDS);
        while (true) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("timed out after " + INDEX_RPC_TIMEOUT_SECONDS
                                + "s draining StreamIndexDocuments responses (got "
                                + collected.size() + " of " + requests.size() + ")")
                        .asRuntimeException();
            }
            Object next;
            try {
                next = responses.poll(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED.withCause(e)
                        .withDescription("interrupted draining StreamIndexDocuments responses")
                        .asRuntimeException();
            }
            if (next == null) continue;
            if (next == COMPLETE) {
                return collected;
            }
            if (next instanceof Throwable t) {
                if (t instanceof StatusRuntimeException sre) throw sre;
                if (t instanceof RuntimeException re) throw re;
                throw new RuntimeException(t);
            }
            collected.add((StreamIndexDocumentsResponse) next);
        }
    }

    /**
     * Open a per-call bidi stream to the manager's {@code StreamIndexDocuments},
     * send one request, await one response, close the stream. Plain blocking;
     * the caller is expected to be on a virtual thread.
     */
    StreamIndexDocumentsResponse streamOne(StreamIndexDocumentsRequest request) {
        CompletableFuture<StreamIndexDocumentsResponse> future = new CompletableFuture<>();
        StreamObserver<StreamIndexDocumentsResponse> respObs = new StreamObserver<>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse v) {
                future.complete(v);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!future.isDone()) {
                    future.completeExceptionally(Status.INTERNAL
                            .withDescription("manager StreamIndexDocuments closed without a response")
                            .asRuntimeException());
                }
            }
        };

        StreamObserver<StreamIndexDocumentsRequest> reqObs = managerStub.streamIndexDocuments(respObs);
        try {
            reqObs.onNext(request);
            reqObs.onCompleted();
        } catch (RuntimeException e) {
            // If the half-close throws, surface a clean status so callers see
            // the underlying transport problem rather than a half-future.
            future.completeExceptionally(e);
        }

        try {
            return future.get(INDEX_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withCause(e)
                    .withDescription("interrupted awaiting StreamIndexDocuments response")
                    .asRuntimeException();
        } catch (TimeoutException e) {
            throw Status.DEADLINE_EXCEEDED.withCause(e)
                    .withDescription("timed out after " + INDEX_RPC_TIMEOUT_SECONDS
                            + "s awaiting StreamIndexDocuments response")
                    .asRuntimeException();
        }
    }
}
