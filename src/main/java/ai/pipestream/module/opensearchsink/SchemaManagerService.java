package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.module.opensearchsink.config.IndexingStrategy;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
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
     * Ensures that the target index and all its semantic side indices are provisioned.
     * This is an "Eager Provisioning" step that should be called before high-throughput indexing.
     *
     * @param indexName The target index name
     * @param document The document whose semantic sets should be provisioned
     * @param documentType Optional hint for schema generation
     * @return A Uni that completes when provisioning is finished (or skipped due to cache hit)
     */
    public Uni<Void> ensureIndexProvisioned(String indexName, PipeDoc document, String documentType) {
        if (!document.hasSearchMetadata()) {
            return Uni.createFrom().voidItem();
        }

        Set<String> semanticConfigIds = semanticConfigIds(document);
        String cacheKey = provisioningCacheKey(indexName, semanticConfigIds);

        if (provisionedIndexCache.getIfPresent(cacheKey) != null) {
            return Uni.createFrom().voidItem();
        }
        RuntimeException cachedFailure = failedProvisioningCache.getIfPresent(cacheKey);
        if (cachedFailure != null) {
            return Uni.createFrom().failure(cachedFailure);
        }

        LOG.infof("Eagerly provisioning index %s for semantic configs: %s", indexName, semanticConfigIds);

        ProvisionIndexRequest.Builder requestBuilder = ProvisionIndexRequest.newBuilder()
                .setIndexName(indexName)
                .addAllSemanticConfigIds(semanticConfigIds);

        if (documentType != null && !documentType.isBlank()) {
            requestBuilder.setDocumentType(documentType);
        }

        return grpcClientFactory.getClient(managerServiceName, MutinyOpenSearchManagerServiceGrpc::newMutinyStub)
                .flatMap(client -> client.provisionIndex(requestBuilder.build()))
                .onItem().transformToUni(resp -> {
                    if (resp.getSuccess()) {
                        LOG.infof("Successfully provisioned index %s: %s", indexName, resp.getMessage());
                        provisionedIndexCache.put(cacheKey, true);
                        failedProvisioningCache.invalidate(cacheKey);
                        return Uni.createFrom().voidItem();
                    } else {
                        RuntimeException failure = new RuntimeException(
                                "Index provisioning failed for " + indexName + ": " + resp.getMessage());
                        failedProvisioningCache.put(cacheKey, failure);
                        return Uni.createFrom().failure(failure);
                    }
                })
                .onFailure().invoke(t -> LOG.errorf(t, "Error calling ProvisionIndex for %s", indexName))
                .replaceWithVoid();
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

                    return grpcClientFactory.getClient(managerServiceName, MutinyOpenSearchManagerServiceGrpc::newMutinyStub)
                            .flatMap(client -> client.indexDocument(requestBuilder.build()))
                            .onItem().transformToUni(resp -> {
                                if (resp.getSuccess()) {
                                    return Uni.createFrom().item(resp.getMessage());
                                } else {
                                    return Uni.createFrom().failure(new RuntimeException(resp.getMessage()));
                                }
                            });
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
