package ai.pipestream.module.opensearchsink;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service responsible for managing OpenSearch index schemas.
 * For Phase 1, this is a minimal implementation that handles index naming.
 * In the future, this will coordinate with opensearch-manager via gRPC to ensure
 * dynamic schema creation for nested embeddings fields.
 */
@ApplicationScoped
public class SchemaManagerService {

    private static final Logger LOG = Logger.getLogger(SchemaManagerService.class);

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
     * Ensures that the specified index exists with the proper schema.
     * For Phase 1, this is a no-op that just returns success.
     * In the future, this will:
     * 1. Check if the index exists
     * 2. If not, create it with the nested embeddings mapping
     * 3. Ensure the vector field(s) are properly configured based on embedding configs
     * 4. Coordinate with opensearch-manager via gRPC for distributed schema management
     * 
     * @param indexName The name of the index to ensure exists
     * @return A Uni that completes when the index is ready
     */
    public Uni<Void> ensureIndexExists(String indexName) {
        // Phase 1: No-op - assume index exists or will be created externally
        // Phase 2: Will call opensearch-manager gRPC service to ensure schema
        LOG.debugf("Ensuring index exists: %s (no-op for Phase 1)", indexName);
        return Uni.createFrom().voidItem();
    }
}

