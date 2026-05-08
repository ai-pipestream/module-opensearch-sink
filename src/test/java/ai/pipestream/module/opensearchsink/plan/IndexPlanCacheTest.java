package ai.pipestream.module.opensearchsink.plan;

import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link IndexPlanCache} that exercise the resolution
 * contract without spinning up a real {@code IndexPlanService} gRPC server.
 * The cache exposes {@link IndexPlanCache#setFetcher} as a test seam so we
 * inject an in-memory map keyed by plan id.
 *
 * <p>Each test asserts the public contract exhaustively:
 * <ul>
 *   <li>READY plan → resolves OK.</li>
 *   <li>FAILED / PENDING plan → fails loud with the plan id and status named.</li>
 *   <li>Missing plan ({@link IndexPlanCache.FetchOutcome#missing()}) → fails loud
 *       with the plan id named.</li>
 *   <li>Multiple plan_ids, one missing → all-or-nothing failure.</li>
 *   <li>Empty plan_ids list → fails loud.</li>
 * </ul>
 */
@QuarkusTest
public class IndexPlanCacheTest {

    @Inject
    IndexPlanCache cache;

    private Map<String, IndexPlanCache.FetchOutcome> fakePlans;

    @BeforeEach
    void setUp() {
        fakePlans = new HashMap<>();
        cache.setFetcher(id -> {
            IndexPlanCache.FetchOutcome o = fakePlans.get(id);
            return o == null ? IndexPlanCache.FetchOutcome.missing() : o;
        });
        cache.invalidateAll();
    }

    private IndexPlan plan(String id, IndexPlanStatus status) {
        return IndexPlan.newBuilder()
                .setId(id)
                .setName("plan-" + id)
                .setIndexName("idx-" + id)
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .addVectorSetIds("vs-" + id)
                .setStatus(status)
                .build();
    }

    @Test
    void readyPlanResolvesAndIsCached() {
        fakePlans.put("p1", IndexPlanCache.FetchOutcome.found(plan("p1", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));

        List<ResolvedPlan> resolved = cache.resolve(List.of("p1")).await().indefinitely();

        assertThat(resolved)
                .as("READY plan should resolve into a single ResolvedPlan with the expected fields")
                .hasSize(1);
        assertThat(resolved.get(0).id()).as("resolved plan id").isEqualTo("p1");
        assertThat(resolved.get(0).indexName()).as("resolved plan indexName").isEqualTo("idx-p1");
        assertThat(resolved.get(0).strategy())
                .as("resolved plan strategy mirrors the IndexPlan proto")
                .isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
        assertThat(resolved.get(0).vectorSetIds())
                .as("resolved plan vectorSetIds carry the proto membership")
                .containsExactly("vs-p1");
    }

    @Test
    void cacheHitSkipsFetcherOnSecondCall() {
        AtomicInteger calls = new AtomicInteger();
        cache.setFetcher(id -> {
            calls.incrementAndGet();
            return IndexPlanCache.FetchOutcome.found(plan(id, IndexPlanStatus.INDEX_PLAN_STATUS_READY));
        });

        cache.resolve(List.of("p1")).await().indefinitely();
        cache.resolve(List.of("p1")).await().indefinitely();

        assertThat(calls.get())
                .as("a second resolve of the same plan id must be served from cache (one fetcher call only)")
                .isEqualTo(1);
    }

    @Test
    void failedPlanFailsLoudWithPlanIdAndStatus() {
        fakePlans.put("p-bad", IndexPlanCache.FetchOutcome.found(
                plan("p-bad", IndexPlanStatus.INDEX_PLAN_STATUS_FAILED).toBuilder()
                        .setLastError("knn provisioning blew up")
                        .build()));

        assertThatThrownBy(() -> cache.resolve(List.of("p-bad")).await().indefinitely())
                .as("FAILED plan must fail loud naming the plan id and status")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("p-bad")
                .hasMessageContaining("INDEX_PLAN_STATUS_FAILED")
                .hasMessageContaining("knn provisioning blew up");
    }

    @Test
    void pendingPlanFailsLoudWithPlanIdAndStatus() {
        fakePlans.put("p-pend", IndexPlanCache.FetchOutcome.found(
                plan("p-pend", IndexPlanStatus.INDEX_PLAN_STATUS_PENDING)));

        assertThatThrownBy(() -> cache.resolve(List.of("p-pend")).await().indefinitely())
                .as("PENDING plan is not READY — sink must reject")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("p-pend")
                .hasMessageContaining("INDEX_PLAN_STATUS_PENDING");
    }

    @Test
    void missingPlanFailsLoudWithPlanIdNamed() {
        // No entry for "p-missing" → fetcher returns Missing()
        assertThatThrownBy(() -> cache.resolve(List.of("p-missing")).await().indefinitely())
                .as("missing plan id must fail loud and name the offending id")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("missing")
                .hasMessageContaining("p-missing");
    }

    @Test
    void multiplePlanIdsOneMissingFailsAtomically() {
        fakePlans.put("p-ok", IndexPlanCache.FetchOutcome.found(plan("p-ok", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));
        // p-missing not registered → fetcher returns Missing()

        assertThatThrownBy(() -> cache.resolve(List.of("p-ok", "p-missing")).await().indefinitely())
                .as("a single missing plan must reject the whole batch (no partial success)")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("p-missing");
    }

    @Test
    void multiplePlansMixedFailuresAllReported() {
        fakePlans.put("p-ok", IndexPlanCache.FetchOutcome.found(plan("p-ok", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));
        fakePlans.put("p-failed", IndexPlanCache.FetchOutcome.found(plan("p-failed", IndexPlanStatus.INDEX_PLAN_STATUS_FAILED)));
        // p-missing → Missing()

        assertThatThrownBy(() -> cache.resolve(List.of("p-ok", "p-failed", "p-missing")).await().indefinitely())
                .as("a single PlanResolutionException must enumerate all offenders so operators see the full picture")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("p-failed")
                .hasMessageContaining("p-missing");
    }

    @Test
    void emptyPlanIdsFailsLoud() {
        assertThatThrownBy(() -> cache.resolve(List.of()).await().indefinitely())
                .as("empty plan_ids must reject — sink config requires at least one plan")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("plan_ids is empty");
    }

    @Test
    void nullPlanIdsFailsLoud() {
        assertThatThrownBy(() -> cache.resolve(null).await().indefinitely())
                .as("null plan_ids must reject — sink config requires at least one plan")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("plan_ids is empty");
    }

    @Test
    void blankPlanIdInListFailsLoud() {
        assertThatThrownBy(() -> cache.resolve(List.of("   ")).await().indefinitely())
                .as("blank plan id must reject — every plan id must be a non-empty string")
                .isInstanceOf(PlanResolutionException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void resolvedListPreservesInputOrder() {
        fakePlans.put("p1", IndexPlanCache.FetchOutcome.found(plan("p1", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));
        fakePlans.put("p2", IndexPlanCache.FetchOutcome.found(plan("p2", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));
        fakePlans.put("p3", IndexPlanCache.FetchOutcome.found(plan("p3", IndexPlanStatus.INDEX_PLAN_STATUS_READY)));

        List<ResolvedPlan> resolved = cache.resolve(List.of("p3", "p1", "p2")).await().indefinitely();

        assertThat(resolved)
                .as("resolved list must preserve input order so plan-by-index access is meaningful")
                .extracting(ResolvedPlan::id)
                .containsExactly("p3", "p1", "p2");
    }
}
