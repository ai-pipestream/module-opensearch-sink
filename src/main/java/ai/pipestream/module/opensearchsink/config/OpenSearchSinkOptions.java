package ai.pipestream.module.opensearchsink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The top-level configuration object for the OpenSearch Sink module.
 * This class defines all settings related to how documents are indexed and how vector search is performed.
 */
@Schema(
    name = "OpenSearchSinkOptions", 
    description = "The top-level configuration for the OpenSearch Sink module. Controls how documents are indexed " +
                 "and how vector search is performed. These settings determine the organization of indexes and " +
                 "the default vector search behavior when not overridden by upstream services."
)
public record OpenSearchSinkOptions(
    @JsonProperty("indexing_strategy")
    @Schema(
        description = "Defines how indexes should be named and organized. Controls whether documents are stored " +
                     "in a single index or separated by document type, and how index names are constructed. " +
                     "This setting affects how data is physically stored in OpenSearch.",
        required = true
    )
    IndexingStrategy indexingStrategy,

    @JsonProperty("default_method")
    @Schema(
        description = "The default k-NN method and parameters to use when not specified by an upstream service. " +
                     "This defines the vector similarity algorithm, search engine, and performance parameters " +
                     "that will be used for vector search operations. These settings can be overridden by " +
                     "specific requests if needed.",
        required = true
    )
    KnnMethod defaultMethod
) {}
