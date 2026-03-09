package ai.pipestream.module.opensearchsink.config;

import io.smallrye.config.ConfigMapping;

/**
 * JVM-level operational configuration for the OpenSearch sink.
 * This is currently empty as all business logic is managed via JSONForms (OpenSearchSinkOptions).
 */
@ConfigMapping(prefix = "opensearch.sink")
public interface OpenSearchSinkConfig {
    // Intentionally empty. Add technical JVM settings here if needed (e.g., global timeouts).
}
