package ai.pipestream.module.opensearchsink.config;

import ai.pipestream.module.opensearchsink.config.opensearch.BatchOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Top-level configuration for the OpenSearch Sink module node.
 * Controls which index this node writes to and how documents are organized.
 * One sink node = one index.
 */
@Schema(name = "OpenSearchSinkOptions",
        description = "Configuration for the OpenSearch Sink module. " +
                     "Each sink node owns exactly one index and controls how documents are organized and batched.")
public record OpenSearchSinkOptions(
    @JsonProperty("opensearch_instance")
    @Schema(description = "Consul service name of the target OpenSearch instance.",
            required = true,
            examples = {"opensearch-cluster", "search-prod"})
    String opensearchInstance,

    @JsonProperty("index_name")
    @Schema(description = "The index this sink node writes to. Each sink node owns exactly one index.",
            required = true,
            examples = {"pipeline-articles", "pipeline-products"})
    String indexName,

    @JsonProperty("indexing_strategy")
    @Schema(description = "How documents and their embeddings are physically organized in the index.",
            required = true,
            defaultValue = "NESTED")
    IndexingStrategy indexingStrategy,

    @JsonProperty("index_settings")
    @Schema(description = "Physical index settings applied when the index is created.")
    IndexSettings indexSettings,

    @JsonProperty("batch_options")
    @Schema(description = "Controls how documents are batched for bulk indexing.")
    BatchOptions batchOptions
) {}
