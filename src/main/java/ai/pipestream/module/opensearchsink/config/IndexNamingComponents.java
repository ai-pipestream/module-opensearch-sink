package ai.pipestream.module.opensearchsink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Defines the components used to dynamically construct an index name.
 * This is an advanced configuration that most users should not need to modify.
 */
@Schema(name = "IndexNamingComponents",
        description = "Defines the parts used to construct a dynamic index name. The final name is built by joining the enabled components with the delimiter. " +
                      "Example: prefix + delimiter + documentType + delimiter + sourceField + ... " +
                      "NOTE: This is an advanced configuration. Most users should keep the default values to ensure " +
                      "compatibility with standard search APIs and consistent naming conventions.")
public record IndexNamingComponents(
    @JsonProperty("prefix")
    @Schema(description = "A static prefix for all generated index names. This will be the first component of every index name " +
                         "and helps identify indexes created by this application.",
            required = true,
            defaultValue = "pipeline",
            examples = {"pipeline", "app", "data"})
    String prefix,

    @JsonProperty("include_document_type")
    @Schema(description = "If true, the document's type (e.g., 'article', 'product') will be included in the index name. " +
                         "This helps organize indexes by content type and is recommended for most use cases. " +
                         "Only disable this if you have a specific reason to group different document types together.",
            defaultValue = "true",
            required = false)
    boolean includeDocumentType,

    @JsonProperty("include_source_field")
    @Schema(description = "If true, the source field that was processed (e.g., 'body', 'title') will be included in the index name. " +
                         "This helps distinguish between indexes containing embeddings from different fields of the same document type. " +
                         "Recommended for advanced use cases with multiple vector fields per document.",
            defaultValue = "true",
            required = false)
    boolean includeSourceField,

    @JsonProperty("include_embedding_id")
    @Schema(description = "If true, the ID of the embedding model (e.g., 'minilm_l6_v2') will be included in the index name. " +
                         "This helps distinguish between indexes containing embeddings from different models. " +
                         "Recommended when experimenting with multiple embedding models for the same content.",
            defaultValue = "true",
            required = false)
    boolean includeEmbeddingId,

    @JsonProperty("delimiter")
    @Schema(description = "The single character used to separate the components of the index name. " +
                         "Choose a character that won't appear in your document types or field names to avoid ambiguity. " +
                         "Common choices include hyphens, underscores, and dots.",
            defaultValue = "-",
            required = true,
            pattern = "^[^a-zA-Z0-9\\s]$",
            examples = {"-", "_", "."})
    String delimiter
) {}
