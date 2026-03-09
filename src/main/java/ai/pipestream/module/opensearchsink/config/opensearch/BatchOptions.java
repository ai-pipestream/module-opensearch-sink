package ai.pipestream.module.opensearchsink.config.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Configuration for how documents are batched and sent to OpenSearch for indexing.
 */
@Schema(name = "OpenSearchBatchOptions",
        description = "Controls how documents are batched and sent to OpenSearch for indexing. " +
                     "These settings affect throughput and latency of the indexing process.")
public record BatchOptions(
    @JsonProperty(value = "max_batch_size", defaultValue = "100")
    @Schema(description = "Maximum number of documents to include in a single bulk request to OpenSearch.",
            defaultValue = "100", minimum = "1", maximum = "10000", required = true,
            examples = {"50", "100", "1000"})
    int maxBatchSize,

    @JsonProperty(value = "max_time_window_ms", defaultValue = "500")
    @Schema(description = "Maximum time in milliseconds to buffer documents before sending a bulk request.",
            defaultValue = "500", minimum = "10", maximum = "60000", required = true,
            examples = {"100", "500", "1000"})
    int maxTimeWindowMs
) {
    public BatchOptions() {
        this(100, 500);
    }
}
