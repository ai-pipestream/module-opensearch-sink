package ai.pipestream.module.opensearchsink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Physical index settings for the OpenSearch index created by this sink node.
 */
@Schema(name = "IndexSettings",
        description = "Physical index settings for the OpenSearch index managed by this sink node. " +
                      "These settings are applied when the index is created via opensearch-manager.")
public record IndexSettings(
    @JsonProperty("number_of_shards")
    @Schema(description = "Number of primary shards for the index.",
            defaultValue = "1", minimum = "1", required = true)
    int numberOfShards,

    @JsonProperty("number_of_replicas")
    @Schema(description = "Number of replica shards for data redundancy.",
            defaultValue = "1", minimum = "0", required = true)
    int numberOfReplicas,

    @JsonProperty("refresh_interval")
    @Schema(description = "How often the index is refreshed, making recent changes visible to search. " +
                         "Use '-1' to disable automatic refresh during bulk loading.",
            defaultValue = "1s", examples = {"1s", "5s", "30s", "-1"}, required = true)
    String refreshInterval
) {
    public IndexSettings() {
        this(1, 1, "1s");
    }
}
