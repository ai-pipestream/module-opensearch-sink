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
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
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
 * Sink → manager indexing uses ONE long-lived bidi {@code StreamIndexDocuments}
 * stream held for the bean lifetime. Per-doc indexing calls register a
 * {@link CompletableFuture} in {@link #pending} keyed by {@code request_id},
 * push the request onto the persistent stream, and block their virtual thread
 * waiting on the future. The shared response observer routes each manager
 * response back to the matching future by {@code request_id}. The manager's
 * server-side batcher ({@code group().intoLists().of(100, 500ms)}) is designed
 * for exactly this shape — one stream feeding many docs — so per-doc latency
 * stays bounded by the batch flush interval, not by per-call open/close
 * handshakes.
 * <p>
 * On stream failure the request observer is dropped, every in-flight future
 * is failed with the cause, and the next call lazily reopens the stream. Plain
 * async-callback gRPC stub; no Mutiny anywhere on this path.
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);

    /** Per-call deadline on the sink→manager round-trip. */
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

    /**
     * Per-doc correlation: {@code request_id → future-of-response}. Insert
     * happens-before {@code reqObs.onNext(req)} happens-before manager response,
     * so the response observer's dispatcher always sees the entry when the
     * matching response arrives. Cleared on stream failure.
     */
    final ConcurrentHashMap<String, CompletableFuture<StreamIndexDocumentsResponse>> pending =
            new ConcurrentHashMap<>();

    /**
     * The persistent request-side observer for the long-lived bidi. Volatile
     * because {@link #handleStreamFailure} replaces it with {@code null} on
     * disconnect and {@link #ensureStream} rebuilds it. gRPC
     * {@link StreamObserver#onNext} is NOT thread-safe, so concurrent senders
     * must serialise on {@link #writeLock}.
     */
    private volatile StreamObserver<StreamIndexDocumentsRequest> reqObs;

    /** Serialises {@code reqObs.onNext(...)} across concurrent VT callers. */
    private final Object writeLock = new Object();

    /** Set in {@link #close()} to suppress reconnect during shutdown. */
    private volatile boolean shutdown = false;

    @PostConstruct
    void init() {
        if (managerStub == null && managerChannel != null) {
            managerStub = OpenSearchManagerServiceGrpc.newStub(managerChannel);
        }
        // Eagerly open the persistent stream so the first indexing call
        // doesn't pay the open-handshake latency.
        ensureStream();
    }

    @PreDestroy
    void close() {
        shutdown = true;
        StreamObserver<StreamIndexDocumentsRequest> obs = reqObs;
        reqObs = null;
        if (obs != null) {
            try {
                synchronized (writeLock) {
                    obs.onCompleted();
                }
            } catch (RuntimeException ignored) {
                // half-close best-effort during shutdown
            }
        }
        StatusRuntimeException cancel = Status.CANCELLED
                .withDescription("sink shutting down — manager stream closed")
                .asRuntimeException();
        for (var entry : pending.entrySet()) {
            entry.getValue().completeExceptionally(cancel);
        }
        pending.clear();
    }

    /**
     * Returns the live request observer, opening a new persistent stream if
     * one is not currently held. Synchronised so concurrent VT callers can't
     * race to open multiple streams during a reconnect.
     */
    private StreamObserver<StreamIndexDocumentsRequest> ensureStream() {
        StreamObserver<StreamIndexDocumentsRequest> obs = reqObs;
        if (obs != null) return obs;
        synchronized (this) {
            if (reqObs == null && !shutdown) {
                openStream();
            }
            return reqObs;
        }
    }

    /**
     * Builds a fresh response observer that dispatches each {@code onNext} to
     * the matching {@link #pending} future by {@code request_id}, and opens a
     * new bidi against the manager. Caller holds {@code synchronized (this)}.
     */
    private void openStream() {
        if (managerStub == null) {
            throw Status.UNAVAILABLE
                    .withDescription("manager stub not initialised (no channel)")
                    .asRuntimeException();
        }
        StreamObserver<StreamIndexDocumentsResponse> respObs = new StreamObserver<>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse v) {
                CompletableFuture<StreamIndexDocumentsResponse> f = pending.remove(v.getRequestId());
                if (f != null) {
                    f.complete(v);
                } else {
                    // Possible if the future was already failed by reconnect
                    // and the dead stream's late onNext arrived after.
                    LOG.debugf("dropped manager response for unknown request_id=%s", v.getRequestId());
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.warnf(t, "manager StreamIndexDocuments errored — dropping stream, will reopen on next call");
                handleStreamFailure(t);
            }

            @Override
            public void onCompleted() {
                LOG.warnf("manager half-closed StreamIndexDocuments unexpectedly — will reopen on next call");
                handleStreamFailure(Status.UNAVAILABLE
                        .withDescription("manager half-closed StreamIndexDocuments")
                        .asRuntimeException());
            }
        };
        reqObs = managerStub.streamIndexDocuments(respObs);
        LOG.debug("opened persistent StreamIndexDocuments to manager");
    }

    /**
     * Drops the current request observer and fails every in-flight future.
     * Lazy reconnect: the next {@link #ensureStream} call rebuilds the stream.
     * Idempotent — concurrent failure callbacks (onError + late onNext) are
     * safe.
     */
    private void handleStreamFailure(Throwable cause) {
        reqObs = null;
        // Drain pending — any future not yet completed by a real response is
        // failed with the disconnect cause so its caller sees a clean status.
        var inFlight = new ArrayList<>(pending.values());
        pending.clear();
        for (CompletableFuture<StreamIndexDocumentsResponse> f : inFlight) {
            if (!f.isDone()) {
                f.completeExceptionally(cause);
            }
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
     * Send a batch of indexing requests onto the persistent bidi stream and
     * return their responses in REQUEST order (response[i] corresponds to
     * requests[i]). Each request is registered for correlation by
     * {@code request_id} and dispatched to the shared response observer
     * exactly like a single-doc call — this method is just a convenience
     * "submit N, await N" wrapper around the same persistent stream.
     * <p>
     * Used by the experimental streaming ingestion path
     * ({@code OpenSearchIngestionServiceImpl.streamDocuments}). Tests override
     * this hook to record batch sizes without spinning up a real manager.
     */
    public List<StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
            List<StreamIndexDocumentsRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return List.of();
        }
        // Experimental ingestion opens one stream per micro-batch so the
        // manager (and WireMock in tests) can half-close after each batch.
        // The persistent stream in {@link #streamOne} is for steady per-doc
        // indexing on the production path.
        BlockingQueue<Object> responses = new LinkedBlockingQueue<>();
        Object completeMarker = new Object();
        StreamObserver<StreamIndexDocumentsResponse> respObs = new StreamObserver<>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse v) {
                responses.add(v);
            }

            @Override
            public void onError(Throwable t) {
                responses.add(t);
            }

            @Override
            public void onCompleted() {
                responses.add(completeMarker);
            }
        };
        StreamObserver<StreamIndexDocumentsRequest> batchReqObs =
                managerStub.streamIndexDocuments(respObs);
        try {
            for (StreamIndexDocumentsRequest req : requests) {
                batchReqObs.onNext(req);
            }
            batchReqObs.onCompleted();
        } catch (RuntimeException sendError) {
            try {
                batchReqObs.onError(sendError);
            } catch (RuntimeException ignored) {
                // already failed; surface the original
            }
            throw sendError;
        }

        List<StreamIndexDocumentsResponse> collected = new ArrayList<>(requests.size());
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(INDEX_RPC_TIMEOUT_SECONDS);
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
                next = responses.poll(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED.withCause(e)
                        .withDescription("interrupted draining StreamIndexDocuments responses")
                        .asRuntimeException();
            }
            if (next == null) {
                continue;
            }
            if (next == completeMarker) {
                return collected;
            }
            if (next instanceof Throwable t) {
                if (t instanceof StatusRuntimeException sre) {
                    throw sre;
                }
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException(t);
            }
            collected.add((StreamIndexDocumentsResponse) next);
        }
    }

    /**
     * Push a single request onto the persistent bidi stream and await its
     * matching response. Correlation is by {@code request_id} via
     * {@link #pending}; the shared response observer dispatches manager
     * replies back to this future. Caller is expected to be on a virtual
     * thread — the {@link CompletableFuture#get} parks the VT cleanly.
     */
    StreamIndexDocumentsResponse streamOne(StreamIndexDocumentsRequest request) {
        StreamObserver<StreamIndexDocumentsRequest> obs = ensureStream();
        if (obs == null) {
            throw Status.UNAVAILABLE
                    .withDescription("manager StreamIndexDocuments not open")
                    .asRuntimeException();
        }

        String requestId = request.getRequestId();
        CompletableFuture<StreamIndexDocumentsResponse> future = new CompletableFuture<>();
        if (pending.putIfAbsent(requestId, future) != null) {
            throw new IllegalStateException("duplicate request_id in pending map: " + requestId);
        }

        try {
            synchronized (writeLock) {
                obs.onNext(request);
            }
        } catch (RuntimeException sendError) {
            pending.remove(requestId);
            handleStreamFailure(sendError);
            throw sendError;
        }

        try {
            return future.get(INDEX_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            pending.remove(requestId);
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            pending.remove(requestId);
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withCause(e)
                    .withDescription("interrupted awaiting StreamIndexDocuments response")
                    .asRuntimeException();
        } catch (TimeoutException e) {
            pending.remove(requestId);
            throw Status.DEADLINE_EXCEEDED.withCause(e)
                    .withDescription("timed out after " + INDEX_RPC_TIMEOUT_SECONDS
                            + "s awaiting StreamIndexDocuments response (request_id=" + requestId + ")")
                    .asRuntimeException();
        }
    }
}
