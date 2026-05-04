package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.module.opensearchsink.config.IndexingStrategy;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.grpc.GrpcFutures;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service responsible for managing OpenSearch index schemas via the OpenSearch Manager.
 * Delegates strictly to OpenSearchManagerService for organic registration and indexing.
 * Uses DynamicGrpcClientFactory for Consul-based service discovery (consistent with engine pattern).
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);

    /**
     * Single-thread-per-task virtual-thread executor used to host blocking
     * gRPC calls outside the Vert.x event loop. Same shape as
     * module-testing-sidecar's {@code BLOCKING_GRPC_EXECUTOR}. Required for
     * the {@link GrpcFutures}-based plain-async-stub call sites that replace
     * the Mutiny stubs prone to "CANCELLED: Context cancelled without error"
     * when the caller's Vert.x context is invalidated mid-call.
     */
    private static final java.util.concurrent.ExecutorService BLOCKING_GRPC_EXECUTOR =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    @Inject
    ChunkDocumentConverter chunkDocumentConverter;

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @ConfigProperty(name = "module.opensearch-sink.manager-service", defaultValue = "opensearch-manager")
    String managerServiceName;

    /**
     * Cache of indices that have been successfully provisioned during this lifecycle.
     * Key is "indexName|configId1,configId2,...".
     */
    private Cache<String, Boolean> provisionedIndexCache;

    /**
     * Short-lived cache of provisioning failures. This keeps a broken manager/config
     * from receiving one ProvisionIndex RPC per document while still retrying soon.
     */
    private Cache<String, RuntimeException> failedProvisioningCache;

    @PostConstruct
    void init() {
        provisionedIndexCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(1000)
                .build();
        failedProvisioningCache = Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(1000)
                .build();
    }

    /**
     * No-op. The sink no longer eagerly provisions per doc — admin paths
     * ({@code AssignSemanticConfigToIndex}, {@code BindVectorSetToIndex},
     * {@code ProvisionIndex}) are the single source of truth for index
     * topology, and they must be called before any doc is indexed. If the
     * index/field isn't pre-provisioned, the manager-side
     * {@code IndexKnnProvisioner.requireKnnField} fails the doc loud with a
     * clear pointer.
     *
     * <p>Previously this method called {@code ProvisionIndex} per
     * (index, semanticConfigs) pair on first encounter of each cache key,
     * adding a gRPC round trip + a worst-case 2-4 cluster-state OpenSearch
     * round trips to the first doc per (JVM, index, configs). On a cold
     * cache that was the difference between "thousands of docs/sec" and
     * "seconds per doc". Removed entirely.
     *
     * <p>Kept as a method (returning a completed Uni) so existing call
     * sites compile without churn during the migration. Once all callers
     * are removed it can be deleted.
     */
    public Uni<Void> ensureIndexProvisioned(String indexName, PipeDoc document, String documentType) {
        return Uni.createFrom().voidItem();
    }

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
     * Determines the index name for a given document type.
     * Uses a simple naming convention: "pipeline-{documentType}"
     *
     * @param documentType The document type (e.g., "article", "test-doc")
     * @return The index name
     */
    public String determineIndexName(String documentType) {
        if (documentType == null || documentType.isEmpty()) {
            return "pipeline-documents";
        }
        return "pipeline-" + documentType.toLowerCase();
    }

    /**
     * Proxies the indexing request to the OpenSearch Manager.
     */
    public Uni<String> indexDocumentViaManager(String indexName, PipeDoc document, OpenSearchDocument osDoc, Optional<OpenSearchSinkOptions> options) {
        String docType = document.hasSearchMetadata() ? document.getSearchMetadata().getDocumentType() : null;
        return ensureIndexProvisioned(indexName, document, docType)
                .flatMap(v -> {
                    IndexDocumentRequest.Builder requestBuilder = IndexDocumentRequest.newBuilder()
                            .setIndexName(indexName)
                            .setDocument(osDoc)
                            .setDocumentId(document.getDocId());

                    if (document.hasOwnership()) {
                        requestBuilder.setAccountId(document.getOwnership().getAccountId());
                        requestBuilder.setDatasourceId(document.getOwnership().getDatasourceId());
                    }

                    IndexingStrategy localStrategy = options.map(OpenSearchSinkOptions::indexingStrategy).orElse(null);
                    ai.pipestream.opensearch.v1.IndexingStrategy protoStrategy = mapToProtoStrategy(localStrategy);
                    requestBuilder.setIndexingStrategy(protoStrategy);

                    if (protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED
                            || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES) {

                        ChunkConversionResult chunkResult = chunkDocumentConverter.convertToChunks(
                                document, osDoc, indexName, protoStrategy);

                        requestBuilder.setDocumentMap(chunkResult.documentMap());
                        requestBuilder.addAllChunkDocuments(chunkResult.chunkDocuments());

                        LOG.infof("Enriched IndexDocumentRequest with %d chunk documents for strategy %s (doc %s)",
                                chunkResult.chunkDocuments().size(), protoStrategy, document.getDocId());
                    }

                    // Plain async stub + GrpcFutures + virtual thread.
                    // The Mutiny variant of this call is the one that surfaced
                    // "CANCELLED: io.grpc.Context was cancelled without error"
                    // in the e2e test — the engine cancels the worker context
                    // mid-call, the Mutiny adapter loses the terminal event,
                    // and the doc is reported as failed even though
                    // upstream chunking + embedding completed normally.
                    final IndexDocumentRequest indexRequest = requestBuilder.build();
                    return Uni.createFrom().<String>item(() -> {
                                OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub stub =
                                        grpcClientFactory.getAsyncClient(managerServiceName,
                                                OpenSearchManagerServiceGrpc::newStub);
                                IndexDocumentResponse resp;
                                try {
                                    resp = GrpcFutures.<IndexDocumentResponse>unary(observer ->
                                            stub.indexDocument(indexRequest, observer)).get();
                                } catch (java.util.concurrent.ExecutionException e) {
                                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                                    if (cause instanceof RuntimeException re) throw re;
                                    throw new RuntimeException(cause);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException("Interrupted calling IndexDocument", e);
                                }
                                if (resp.getSuccess()) {
                                    return resp.getMessage();
                                }
                                throw new RuntimeException(resp.getMessage());
                            })
                            .runSubscriptionOn(BLOCKING_GRPC_EXECUTOR);
                });
    }

    /**
     * Maps the local {@link IndexingStrategy} enum to the proto's
     * {@link ai.pipestream.opensearch.v1.IndexingStrategy} enum.
     */
    static ai.pipestream.opensearch.v1.IndexingStrategy mapToProtoStrategy(IndexingStrategy local) {
        if (local == null) {
            return ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED;
        }
        return switch (local) {
            case NESTED -> ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED;
            case PARENT_CHILD -> ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED;
            case CHUNK_COMBINED -> ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED;
            case SEPARATE_INDICES -> ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES;
        };
    }

    /**
     * Proxies a stream of indexing requests to the OpenSearch Manager for high-throughput bulk ingestion.
     */
    public Multi<StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
            Multi<StreamIndexDocumentsRequest> requests) {
        return grpcClientFactory.getClient(managerServiceName, MutinyOpenSearchManagerServiceGrpc::newMutinyStub)
                .onItem().transformToMulti(client -> client.streamIndexDocuments(requests));
    }
}
