package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.module.opensearchsink.config.IndexingStrategy;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
     * Determines the index name for a given document type.
     * Uses a simple naming convention: "pipeline-{documentType}"
     * <p>
     * Note: Used as a fallback when no Engine configuration is provided (e.g. legacy streaming path).
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
     * Proxies the indexing request to the OpenSearch Manager, which handles
     * both schema provisioning (organic registration) and the actual indexing.
     * <p>
     * When the indexing strategy is CHUNK_COMBINED or SEPARATE_INDICES, the request is enriched
     * with a flat document map and chunk documents so the manager can store them directly.
     * For NESTED (the default), the existing single-document flow is used unchanged.
     *
     * @param indexName The target index name
     * @param document The source PipeDoc
     * @param options Optional request-time Sink configuration (includes instance routing and strategy)
     * @return The message from the manager response
     */
    public Uni<String> indexDocumentViaManager(String indexName, PipeDoc document, OpenSearchDocument osDoc, Optional<OpenSearchSinkOptions> options) {
        List<String> auditLogs = new ArrayList<>();

        IndexDocumentRequest.Builder requestBuilder = IndexDocumentRequest.newBuilder()
                .setIndexName(indexName)
                .setDocument(osDoc)
                .setDocumentId(document.getDocId());

        // Use opensearch_instance from options if provided
        options.ifPresent(opt -> {
            if (opt.opensearchInstance() != null && !opt.opensearchInstance().isBlank()) {
                // In a real multi-tenant environment, we would use this to select the gRPC client or
                // add it to the request metadata for the manager to route.
                LOG.debugf("Target OpenSearch instance requested: %s", opt.opensearchInstance());
            }
        });

        if (document.hasOwnership()) {
            requestBuilder.setAccountId(document.getOwnership().getAccountId());
            requestBuilder.setDatasourceId(document.getOwnership().getDatasourceId());
        }

        // Map local indexing strategy to proto enum and enrich the request for flat strategies
        IndexingStrategy localStrategy = options.map(OpenSearchSinkOptions::indexingStrategy).orElse(null);
        ai.pipestream.opensearch.v1.IndexingStrategy protoStrategy = mapToProtoStrategy(localStrategy);
        requestBuilder.setIndexingStrategy(protoStrategy);

        if (protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED
                || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES) {

            ChunkConversionResult chunkResult = chunkDocumentConverter.convertToChunks(
                    document, osDoc, indexName, protoStrategy);

            requestBuilder.setDocumentMap(chunkResult.documentMap());
            requestBuilder.addAllChunkDocuments(chunkResult.chunkDocuments());

            auditLogs.addAll(chunkResult.auditLogs());
            LOG.infof("Enriched IndexDocumentRequest with %d chunk documents for strategy %s (doc %s)",
                    chunkResult.chunkDocuments().size(), protoStrategy, document.getDocId());
        } else {
            auditLogs.add("Using NESTED strategy for document " + document.getDocId()
                    + " — manager will handle nested embedding extraction");
            LOG.debugf("Using NESTED strategy for document %s", document.getDocId());
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
    }

    /**
     * Returns the audit logs from the most recent chunk conversion, if any.
     * This is exposed so the calling service can include them in the response.
     * (Thread-safety note: the sink processes one request at a time per thread.)
     */
    List<String> getLastChunkAuditLogs() {
        // For now, audit logs from chunk conversion are logged inline.
        // Future enhancement: thread-local or return them in the Uni chain.
        return List.of();
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
    public io.smallrye.mutiny.Multi<ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
            io.smallrye.mutiny.Multi<ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest> requests) {
        return grpcClientFactory.getClient(managerServiceName, MutinyOpenSearchManagerServiceGrpc::newMutinyStub)
                .onItem().transformToMulti(client -> client.streamIndexDocuments(requests));
    }
}
