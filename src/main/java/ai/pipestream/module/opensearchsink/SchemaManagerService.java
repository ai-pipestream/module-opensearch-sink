package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.IndexDocumentRequest;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Optional;

/**
 * Service responsible for managing OpenSearch index schemas via the OpenSearch Manager.
 * Delegates strictly to OpenSearchManagerService for organic registration and indexing.
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);

    @Inject
    DocumentConverterService documentConverter;

    @GrpcClient("opensearch-manager")
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerClient;

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
     * 
     * @param indexName The target index name
     * @param document The document to index
     * @param options Optional request-time Sink configuration (includes instance routing)
     * @return The message from the manager response
     */
    public Uni<String> indexDocumentViaManager(String indexName, PipeDoc document, Optional<OpenSearchSinkOptions> options) {
        OpenSearchDocument osDoc = documentConverter.convertToOpenSearchDocument(document);

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

        return openSearchManagerClient.indexDocument(requestBuilder.build())
                .onItem().transformToUni(resp -> {
                    if (resp.getSuccess()) {
                        return Uni.createFrom().item(resp.getMessage());
                    } else {
                        return Uni.createFrom().failure(new RuntimeException(resp.getMessage()));
                    }
                });
    }

    /**
     * Proxies a stream of indexing requests to the OpenSearch Manager for high-throughput bulk ingestion.
     */
    public io.smallrye.mutiny.Multi<ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
            io.smallrye.mutiny.Multi<ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest> requests) {
        return openSearchManagerClient.streamIndexDocuments(requests);
    }
}
