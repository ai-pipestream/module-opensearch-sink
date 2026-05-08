package ai.pipestream.module.opensearchsink.plan;

import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexingStrategy;

import java.util.List;

/**
 * Snapshot of an {@link IndexPlan} as the sink uses it on the hot path.
 * Pulled out of the proto to keep call sites free of nullable proto getters
 * and to memoize the immutable vector_set_ids list once at resolution time.
 *
 * @param id              plan id
 * @param indexName       physical index name (or prefix for SEPARATE_INDICES)
 * @param strategy        indexing strategy from the plan
 * @param vectorSetIds    immutable copy of the plan's vector set ids
 */
public record ResolvedPlan(
    String id,
    String indexName,
    IndexingStrategy strategy,
    List<String> vectorSetIds
) {
    public ResolvedPlan {
        vectorSetIds = vectorSetIds == null ? List.of() : List.copyOf(vectorSetIds);
    }

    public static ResolvedPlan from(IndexPlan plan) {
        return new ResolvedPlan(
                plan.getId(),
                plan.getIndexName(),
                plan.getIndexingStrategy(),
                plan.getVectorSetIdsList()
        );
    }
}
