package ai.pipestream.module.opensearchsink.config;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Determines how documents and their embeddings are physically organized within the target index.
 */
@Schema(name = "IndexingStrategy",
        description = "Determines how documents and their embeddings are physically organized within the target index.")
public enum IndexingStrategy {
    /**
     * All semantic chunks are stored as nested objects within the parent document.
     * This is the default and works well for typical chunk counts.
     */
    @Schema(description = "Semantic chunks are stored as nested objects within the parent document. " +
                          "Simple and efficient for typical chunk counts.")
    NESTED,

    /**
     * Parent and child documents are stored as separate documents linked by a join field.
     * Better suited for documents with very large numbers of chunks.
     */
    @Schema(description = "Parent and child documents are stored as separate documents linked by a join field. " +
                          "Better for documents with hundreds or thousands of chunks.")
    PARENT_CHILD
}
