package ai.pipestream.module.opensearchsink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * DRAFT - A reference point for our design conversation.
 * Defines the strategy for how OpenSearch indexes should be named and organized.
 */
@Schema(name = "IndexingStrategy",
        description = "Defines the strategy for how documents and their vector embeddings are organized within OpenSearch. " +
                      "This controls whether data is consolidated into a single master index or separated into granular, " +
                      "purpose-built indexes based on the processing run.")
public record IndexingStrategy(
    @JsonProperty("granularity")
    @Schema(description = "Determines how documents are distributed across indexes.",
            defaultValue = "INDEX_PER_RUN_COMBINATION",
            required = true)
    Granularity granularity,

    @JsonProperty("naming_components")
    @Schema(description = "The components to include when constructing dynamic index names. " +
                      "NOTE: This is an advanced option. For most use cases, and to ensure compatibility with standard search APIs, " +
                      "it is highly recommended to keep the default naming components. Only change these settings if you are building a custom " +
                      "search application and need full control over index naming.")
    IndexNamingComponents namingComponents
) {
    /**
     * Defines how documents and their embeddings are distributed across OpenSearch indexes.
     */
    @Schema(name = "Granularity",
            description = "Specifies the physical layout of indexes.")
    public enum Granularity {
        /**
         * All embeddings are stored in a single, master index. This is simpler for broad queries but offers less isolation.
         */
        @Schema(description = "All embeddings are stored in a single, master index. Simpler for broad queries, less isolation.")
        SINGLE_MASTER_INDEX,

        /**
         * Creates a separate, highly optimized index for each unique combination of (source field + chunking config + embedding model).
         * This is the recommended default for performance and schema isolation.
         */
        @Schema(description = "Creates a separate index for each unique processing run (source field + chunk/embed config). Recommended default.")
        INDEX_PER_RUN_COMBINATION,

        /**
         * A hybrid approach. Data is written to BOTH a single master index AND to separate, run-specific indexes.
         * This provides the best of both worlds for query flexibility at the cost of increased storage.
         */
        @Schema(description = "Writes data to both a single master index and to run-specific indexes. Flexible but uses more storage.")
        BOTH
    }
}