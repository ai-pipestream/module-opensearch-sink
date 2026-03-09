package ai.pipestream.module.opensearchsink.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Operational defaults for the OpenSearch sink.
 * These are application-level defaults; per-node config comes from OpenSearchSinkOptions via JSONForms.
 */
@ConfigMapping(prefix = "sink")
public interface OpenSearchSinkConfig {

    @WithDefault("documents")
    String indexPrefix();

    @WithDefault("opensearch-cluster")
    String clusterName();

    @WithDefault("default-pipeline")
    String pipelineName();
}
