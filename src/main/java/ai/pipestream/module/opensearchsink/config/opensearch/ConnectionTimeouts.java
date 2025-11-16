package ai.pipestream.module.opensearchsink.config.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.time.Duration;

/**
 * Optional settings to override default OpenSearch client connection parameters.
 * These settings control the timeout behavior of the OpenSearch client connections.
 * Note: Core settings like hosts and protocol are set at startup and cannot be dynamically changed.
 */
@RegisterForReflection
@Schema(
    name = "ConnectionOverrides",
    description = "Optional settings to override default OpenSearch client connection parameters. " +
                 "These settings control the timeout behavior of the OpenSearch client connections. " +
                 "Timeouts are specified as ISO-8601 duration format (e.g., PT10S for 10 seconds, PT1M for 1 minute)."
)
public record ConnectionTimeouts(
    @JsonProperty("socket_timeout")
    @Schema(
        description = "The timeout for waiting for data or a maximum period of inactivity between two consecutive data packets. " +
                     "If no data is received within this time, the connection will be closed. " +
                     "Specified in ISO-8601 duration format (e.g., PT30S for 30 seconds).",
        examples = {"PT10S", "PT30S", "PT1M"},
        defaultValue = "PT30S",
        minimum = "PT1S",
        maximum = "PT5M",
        required = false
    )
    Duration socketTimeout,

    @JsonProperty("connect_timeout")
    @Schema(
        description = "The timeout until a connection with the OpenSearch server is established. " +
                     "If the connection cannot be established within this time, the request will fail. " +
                     "Specified in ISO-8601 duration format (e.g., PT5S for 5 seconds).",
        examples = {"PT5S", "PT10S", "PT30S"},
        defaultValue = "PT5S",
        minimum = "PT1S",
        maximum = "PT1M",
        required = false
    )
    Duration connectTimeout
) {
    // A default, empty constructor is useful for when this is omitted in the configuration.
    public ConnectionTimeouts() {
        this(Duration.ofSeconds(30), Duration.ofSeconds(5));
    }
}