package ai.pipestream.module.opensearchsink.plan;

import ai.pipestream.opensearch.v1.GetIndexPlanRequest;
import ai.pipestream.opensearch.v1.GetIndexPlanResponse;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanServiceGrpc;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Resolves {@code plan_ids[]} from the sink config to {@link ResolvedPlan}
 * snapshots fetched from {@code IndexPlanService} via Stork-discovered gRPC.
 * <p>
 * Behavior:
 * <ul>
 *   <li>Empty {@code plan_ids} → {@link PlanResolutionException} ("at least one plan required").</li>
 *   <li>Plan not found ({@code NOT_FOUND}) → {@link PlanResolutionException} naming the missing id.</li>
 *   <li>Plan present but {@code status != READY} → {@link PlanResolutionException} naming
 *       each offending plan and its actual status.</li>
 *   <li>All-or-nothing: if <em>any</em> plan in the list fails to resolve
 *       cleanly the whole batch is rejected; the message lists every failure
 *       so the operator gets one diagnostic instead of N retries.</li>
 *   <li>Successful resolutions are cached in-process (Caffeine, 10 min TTL,
 *       1k entries) keyed by plan id. Updates to a plan in the manager won't
 *       be picked up until the entry expires; sinks that need fresh state
 *       restart. Acceptable for a registry that rarely churns at runtime.</li>
 * </ul>
 */
@ApplicationScoped
public class IndexPlanCache {

    private static final Logger LOG = Logger.getLogger(IndexPlanCache.class);

    @Inject
    @GrpcClient("opensearch-manager")
    IndexPlanServiceGrpc.IndexPlanServiceBlockingStub indexPlanBlockingStub;

    private Cache<String, ResolvedPlan> cache;

    /**
     * Pluggable fetch strategy. Production uses {@link #grpcFetcher(String)}
     * which calls {@code IndexPlanService.GetIndexPlan} via Stork-discovered
     * gRPC. Tests can swap in an in-memory implementation via
     * {@link #setFetcher(IndexPlanFetcher)} without spinning up a real
     * gRPC server.
     */
    public interface IndexPlanFetcher {
        /**
         * @return the plan response for the given id, or
         *         {@link FetchOutcome#missing()} when the manager reports
         *         {@code NOT_FOUND}.
         */
        FetchOutcome fetch(String planId);
    }

    /**
     * Outcome of a single fetch. Carries either the plan, a "missing"
     * marker, or an error message — modelled explicitly so the cache's
     * aggregation logic can group failures by category before throwing.
     */
    public sealed interface FetchOutcome {
        record Found(IndexPlan plan) implements FetchOutcome {}
        record Missing() implements FetchOutcome {}
        record Error(String message) implements FetchOutcome {}

        static FetchOutcome found(IndexPlan plan) { return new Found(plan); }
        static FetchOutcome missing() { return new Missing(); }
        static FetchOutcome error(String message) { return new Error(message); }
    }

    private IndexPlanFetcher fetcher;

    @PostConstruct
    void init() {
        cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .maximumSize(1000)
                .build();
        if (fetcher == null) {
            fetcher = this::grpcFetcher;
        }
    }

    /**
     * Test seam: substitute the gRPC fetcher with a stub. Production code
     * should never call this.
     */
    public void setFetcher(IndexPlanFetcher fetcher) {
        this.fetcher = fetcher;
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    private FetchOutcome grpcFetcher(String planId) {
        GetIndexPlanRequest req = GetIndexPlanRequest.newBuilder().setId(planId).build();
        GetIndexPlanResponse resp;
        try {
            resp = indexPlanBlockingStub.getIndexPlan(req);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return FetchOutcome.missing();
            }
            return FetchOutcome.error(sre.getMessage() == null ? sre.toString() : sre.getMessage());
        } catch (RuntimeException e) {
            return FetchOutcome.error(e.getMessage() == null ? e.toString() : e.getMessage());
        }
        if (!resp.hasPlan()) {
            return FetchOutcome.missing();
        }
        return FetchOutcome.found(resp.getPlan());
    }

    /**
     * Resolve the given {@code planIds}. Returns a list of {@link ResolvedPlan}
     * in the same order as the input. Throws {@link PlanResolutionException}
     * with a message naming every offending id if any cannot be resolved or
     * is not READY. Empty / null list also throws.
     * <p>
     * Plain blocking method; callers run on a virtual thread so the gRPC
     * round-trip parks cheaply.
     */
    public List<ResolvedPlan> resolve(List<String> planIds) {
        if (planIds == null || planIds.isEmpty()) {
            throw new PlanResolutionException(
                    "OpenSearchSinkOptions.plan_ids is empty; the sink requires at least one IndexPlan reference");
        }

        Map<String, ResolvedPlan> hits = new LinkedHashMap<>();
        List<String> toFetch = new ArrayList<>();
        for (String id : planIds) {
            if (id == null || id.isBlank()) {
                throw new PlanResolutionException(
                        "OpenSearchSinkOptions.plan_ids contains a null or blank entry; "
                                + "every plan id must be a non-empty string");
            }
            ResolvedPlan cached = cache.getIfPresent(id);
            if (cached != null) {
                hits.put(id, cached);
            } else {
                toFetch.add(id);
            }
        }

        if (!toFetch.isEmpty()) {
            List<String> missing = new ArrayList<>();
            List<String> notReady = new ArrayList<>();
            List<String> errors = new ArrayList<>();

            for (String id : toFetch) {
                FetchOutcome outcome = fetcher.fetch(id);
                if (outcome instanceof FetchOutcome.Missing) {
                    missing.add(id);
                    continue;
                }
                if (outcome instanceof FetchOutcome.Error err) {
                    errors.add(id + " (" + err.message() + ")");
                    continue;
                }
                IndexPlan plan = ((FetchOutcome.Found) outcome).plan();
                if (plan.getStatus() != IndexPlanStatus.INDEX_PLAN_STATUS_READY) {
                    notReady.add(id + " (status=" + plan.getStatus().name()
                            + (plan.getLastError().isEmpty() ? "" : ", lastError=\"" + plan.getLastError() + "\"")
                            + ")");
                    continue;
                }
                ResolvedPlan resolved = ResolvedPlan.from(plan);
                cache.put(id, resolved);
                hits.put(id, resolved);
            }

            if (!missing.isEmpty() || !notReady.isEmpty() || !errors.isEmpty()) {
                StringBuilder sb = new StringBuilder("IndexPlan resolution failed:");
                if (!missing.isEmpty()) {
                    sb.append(" missing=").append(missing);
                }
                if (!notReady.isEmpty()) {
                    sb.append(" not_ready=").append(notReady);
                }
                if (!errors.isEmpty()) {
                    sb.append(" errors=").append(errors);
                }
                throw new PlanResolutionException(sb.toString());
            }
        }

        // Preserve input order
        List<ResolvedPlan> ordered = new ArrayList<>(planIds.size());
        for (String id : planIds) {
            ordered.add(hits.get(id));
        }
        LOG.debugf("Resolved %d IndexPlans (cache hits=%d, fetched=%d)",
                ordered.size(), planIds.size() - toFetch.size(), toFetch.size());
        return ordered;
    }

    /**
     * Test/admin hook: drop a specific plan from the cache so the next
     * resolve call hits the manager fresh.
     */
    public void invalidate(String planId) {
        if (cache != null) {
            cache.invalidate(planId);
        }
    }

    /**
     * Test/admin hook: drop every cached plan.
     */
    public void invalidateAll() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }
}
