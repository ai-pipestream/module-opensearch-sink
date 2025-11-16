package ai.pipestream.module.opensearchsink.config.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Configuration options for the OpenSearch Sink service.
 * Controls how documents are batched and sent to OpenSearch for indexing.
 */
@Schema(
    name = "OpenSearchBatchOptions",
    description = "Configuration options for the OpenSearch Sink service that control how documents are batched " +
                 "and sent to OpenSearch for indexing. These settings affect performance, throughput, and latency " +
                 "of the indexing process."
)
public record BatchOptions(

    @JsonProperty(value = "max_batch_size", defaultValue = "100")
    @Schema(
        description = "The maximum number of documents to include in a single bulk request to OpenSearch. " +
                     "Higher values can improve throughput but increase memory usage and the impact of failures. " +
                     "Lower values reduce memory usage but may decrease throughput due to more frequent requests.",
        defaultValue = "100",
        minimum = "1",
        maximum = "10000",
        required = true,
        examples = {"50", "100", "1000"}
    )
    int maxBatchSize,

    @JsonProperty(value = "max_time_window_ms", defaultValue = "500")
    @Schema(
        description = "The maximum time window in milliseconds to buffer documents before sending a bulk request " +
                     "even if the batch size hasn't been reached. Lower values reduce latency but may decrease " +
                     "throughput. Higher values improve throughput but increase latency.",
        defaultValue = "500",
        minimum = "10",
        maximum = "60000",
        required = true,
        examples = {"100", "500", "1000"}
    )
    int maxTimeWindowMs,

    @JsonProperty("connection_overrides")
    @Schema(
        description = "Optional settings to override default OpenSearch client connection parameters. " +
                     "These settings control timeout behavior and can be adjusted based on network conditions " +
                     "and expected response times from the OpenSearch cluster.",
        required = false
    )
    ConnectionTimeouts connectionOverrides
) {
    // This constructor provides default values when no configuration is supplied.
    public BatchOptions() {
        this(100, 500, new ConnectionTimeouts());
    }
}