package ai.pipestream.module.opensearchsink.config;

import ai.pipestream.module.opensearchsink.config.opensearch.BatchOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.List;

/**
 * Top-level configuration for the OpenSearch Sink module node.
 * <p>
 * The sink itself is dumb at runtime: it routes whatever vectors arrive into
 * the indices governed by the IndexPlans referenced via {@link #planIds()},
 * using each plan's strategy to resolve field names. The plan governs the
 * physical index name, indexing strategy, vector_set membership, HNSW knobs,
 * and index settings — see
 * {@code ai.pipestream.opensearch.v1.IndexPlanService}.
 * <p>
 * A sink config must reference at least one plan; the sink fails to start
 * (per-request) if {@code planIds} is empty or any referenced plan is missing
 * or not in {@code READY} status.
 */
@Schema(name = "OpenSearchSinkOptions",
        description = "Configuration for the OpenSearch Sink module. " +
                     "Each sink references one or more IndexPlans which govern the physical index, indexing strategy, and vector field set.")
public record OpenSearchSinkOptions(
    @JsonProperty("opensearch_instance")
    @Schema(description = "Consul service name of the target OpenSearch instance.",
            required = true,
            examples = {"opensearch-cluster", "search-prod"})
    String opensearchInstance,

    @JsonProperty("plan_ids")
    @Schema(description = "IndexPlan ids governing the indices this sink writes to. Must contain at least one entry; " +
                          "every referenced plan must exist and be in READY status or the sink rejects the request.",
            required = true,
            examples = {"[\"plan-articles-v1\"]", "[\"plan-articles-v1\", \"plan-summaries-v1\"]"})
    List<String> planIds,

    @JsonProperty("batch_options")
    @Schema(description = "Controls how documents are batched for bulk indexing.")
    BatchOptions batchOptions
) {
    /**
     * Convenience accessor that returns an immutable, never-null view of
     * {@link #planIds()}. Callers that only need to iterate (or test for
     * empty) should use this instead of guarding on null themselves.
     */
    public List<String> planIdsOrEmpty() {
        return planIds == null ? List.of() : List.copyOf(planIds);
    }
}
