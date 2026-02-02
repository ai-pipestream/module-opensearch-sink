package ai.pipestream.module.opensearchsink.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Sink-specific OpenSearch configuration.
 * Uses opensearch.sink.* prefix to avoid conflicts with pipestream-opensearch extension.
 */
@ConfigMapping(prefix = "opensearch.sink")
public interface OpenSearchSinkConfig {

    @WithDefault("documents")
    String indexPrefix();

    @WithDefault("true")
    boolean useOpensearchManager();

    @WithDefault("hnsw")
    String vectorAlgorithm();

    @WithDefault("cosine")
    String vectorSpaceType();

    @WithDefault("opensearch-cluster")
    String clusterName();

    @WithDefault("default-pipeline")
    String pipelineName();
}
