package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkConfig;
import ai.pipestream.opensearch.v1.MutinyOpenSearchManagerServiceGrpc;
import ai.pipestream.quarkus.opensearch.client.ReactiveOpenSearchClient;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.mapping.KnnVectorProperty;
import org.opensearch.client.opensearch._types.mapping.NestedProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;

import java.util.Map;

/**
 * Service responsible for managing OpenSearch index schemas.
 * Vector dimensions are dynamic: resolved from opensearch-manager (IndexEmbeddingBinding) when available,
 * or derived from the document's embeddings when opensearch-manager is unavailable (e.g. tests).
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);
    private static final String NESTED_FIELD_NAME = "embeddings";

    @Inject
    ReactiveOpenSearchClient reactiveOpenSearchClient;

    @Inject
    OpenSearchSinkConfig openSearchSinkConfig;

    @GrpcClient("opensearch-manager")
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManagerClient;

    boolean useOpenSearchManager() {
        return openSearchSinkConfig.useOpensearchManager();
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
     * Ensures that the specified index exists with the nested embeddings mapping.
     * Vector dimensions are dynamic: from opensearch-manager (IndexEmbeddingBinding) when available,
     * or derived from the document when opensearch-manager is unavailable.
     *
     * @param indexName The name of the index to ensure exists
     * @param document  The document being indexed; used to derive dimension when fallback is needed
     * @return A Uni that completes when the index is ready
     */
    public Uni<Void> ensureIndexExists(String indexName, PipeDoc document) {
        return reactiveOpenSearchClient.indexExists(indexName)
                .onItem().transformToUni(exists -> {
                    if (exists) {
                        LOG.debugf("Index %s already exists", indexName);
                        return Uni.createFrom().voidItem();
                    }
                    return ensureSchemaViaOpenSearchManager(indexName)
                            .onFailure().recoverWithUni(e -> {
                                LOG.debugf(e, "opensearch-manager unavailable for schema, falling back to document-derived dimension");
                                return createIndexWithDocumentDimension(indexName, document);
                            });
                });
    }

    /**
     * Try opensearch-manager first. Dimensions resolved from IndexEmbeddingBinding in DB.
     */
    private Uni<Void> ensureSchemaViaOpenSearchManager(String indexName) {
        if (!useOpenSearchManager()) {
            return Uni.createFrom().failure(new IllegalStateException("opensearch-manager disabled"));
        }
        var request = EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName(indexName)
                .setNestedFieldName(NESTED_FIELD_NAME)
                .build();
        return openSearchManagerClient.ensureNestedEmbeddingsFieldExists(request)
                .onItem().transform(r -> (Void) null);
    }

    /**
     * Fallback: derive dimension from document's first embedding and create index locally.
     */
    private Uni<Void> createIndexWithDocumentDimension(String indexName, PipeDoc document) {
        int dimension = deriveDimensionFromDocument(document);
        if (dimension <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                    "Cannot create index: no embeddings in document and opensearch-manager unavailable. " +
                            "Either configure IndexEmbeddingBinding in opensearch-manager or ensure documents have embeddings."));
        }
        LOG.infof("Creating index %s with nested embeddings field %s (dimension=%d from document)", indexName, NESTED_FIELD_NAME, dimension);
        return createIndexWithDimension(indexName, dimension);
    }

    static int deriveDimensionFromDocument(PipeDoc document) {
        if (document == null || !document.hasSearchMetadata()) {
            return 0;
        }
        for (var result : document.getSearchMetadata().getSemanticResultsList()) {
            for (var chunk : result.getChunksList()) {
                if (chunk.hasEmbeddingInfo() && chunk.getEmbeddingInfo().getVectorCount() > 0) {
                    return chunk.getEmbeddingInfo().getVectorCount();
                }
            }
        }
        return 0;
    }

    private Uni<Void> createIndexWithDimension(String indexName, int dimension) {
        var knnVector = KnnVectorProperty.of(k -> k.dimension(dimension));
        var mapping = new TypeMapping.Builder()
                .properties(NESTED_FIELD_NAME, Property.of(p -> p
                        .nested(NestedProperty.of(n -> n
                                .properties(Map.of(
                                        "vector", Property.of(v -> v.knnVector(knnVector)),
                                        "source_text", Property.of(t -> t.text(TextProperty.of(x -> x))),
                                        "context_text", Property.of(t -> t.text(TextProperty.of(x -> x))),
                                        "chunk_config_id", Property.of(k -> k.keyword(x -> x)),
                                        "embedding_id", Property.of(k -> k.keyword(x -> x)),
                                        "is_primary", Property.of(b -> b.boolean_(x -> x))
                                ))
                        ))
                ))
                .build();

        var settings = new IndexSettings.Builder().knn(true).build();
        var createRequest = new CreateIndexRequest.Builder()
                .index(indexName)
                .settings(settings)
                .mappings(mapping)
                .build();

        return Uni.createFrom().item(() -> {
            try {
                reactiveOpenSearchClient.getClient().indices().create(createRequest);
                return (Void) null;
            } catch (OpenSearchException e) {
                if (e.getMessage() != null && e.getMessage().contains("resource_already_exists_exception")) {
                    LOG.infof("Index %s already exists (concurrent creation), continuing", indexName);
                    return (Void) null;
                }
                throw new RuntimeException("Failed to create index " + indexName, e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create index " + indexName, e);
            }
        }).runSubscriptionOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultWorkerPool());
    }
}

