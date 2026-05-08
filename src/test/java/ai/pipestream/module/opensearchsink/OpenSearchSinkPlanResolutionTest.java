package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.module.v1.MutinyPipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.module.opensearchsink.plan.IndexPlanCache;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.test.support.OpenSearchSinkWireMockTestResource;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Service-level tests for the IndexPlan refactor: processData must
 * <ul>
 *   <li>resolve {@code plan_ids[]} to ResolvedPlan via the cache,</li>
 *   <li>refuse the request when any plan is missing or not READY,</li>
 *   <li>refuse the request when {@code plan_ids[]} is empty,</li>
 *   <li>name every offending plan id in the failure log so operators get
 *       a single diagnostic instead of N retries.</li>
 * </ul>
 *
 * <p>The plan cache fetcher is swapped for an in-memory map so the tests
 * exercise the sink's contract without depending on a real
 * {@code IndexPlanService}. The downstream {@code OpenSearchManagerService}
 * is still mocked via the WireMock test resource — only relevant for the
 * "happy path" assertion that a READY plan reaches the manager.
 */
@QuarkusTest
@QuarkusTestResource(OpenSearchSinkWireMockTestResource.class)
public class OpenSearchSinkPlanResolutionTest {

    @GrpcClient("opensearchSink")
    MutinyPipeStepProcessorServiceGrpc.MutinyPipeStepProcessorServiceStub processor;

    @Inject
    IndexPlanCache planCache;

    private Map<String, IndexPlanCache.FetchOutcome> plans;

    @BeforeEach
    void seedPlans() {
        plans = new HashMap<>();
        planCache.setFetcher(id -> {
            IndexPlanCache.FetchOutcome o = plans.get(id);
            return o == null ? IndexPlanCache.FetchOutcome.missing() : o;
        });
        planCache.invalidateAll();
    }

    private static IndexPlan readyPlan(String id) {
        return IndexPlan.newBuilder()
                .setId(id)
                .setName("plan-" + id)
                .setIndexName("idx-" + id)
                .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                .setStatus(IndexPlanStatus.INDEX_PLAN_STATUS_READY)
                .build();
    }

    private static Struct configWithPlanIds(String... planIds) {
        ListValue.Builder lv = ListValue.newBuilder();
        for (String id : planIds) {
            lv.addValues(Value.newBuilder().setStringValue(id).build());
        }
        return Struct.newBuilder()
                .putFields("opensearch_instance", Value.newBuilder().setStringValue("test-cluster").build())
                .putFields("plan_ids", Value.newBuilder().setListValue(lv).build())
                .build();
    }

    private static ProcessDataRequest requestWithConfig(Struct cfg) {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-plan-test")
                .setSearchMetadata(SearchMetadata.newBuilder().setDocumentType("any-type").build())
                .build();
        return ProcessDataRequest.newBuilder()
                .setDocument(doc)
                .setConfig(ProcessConfiguration.newBuilder().setJsonConfig(cfg).build())
                .build();
    }

    @Test
    void readyPlanResolvesAndProcessDataReachesManager() {
        plans.put("plan-ok", IndexPlanCache.FetchOutcome.found(readyPlan("plan-ok")));

        ProcessDataResponse resp = processor
                .processData(requestWithConfig(configWithPlanIds("plan-ok")))
                .await().indefinitely();

        assertThat(resp.getOutcome())
                .as("READY plan with reachable manager mock must yield SUCCESS")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);
        List<String> messages = resp.getLogEntriesList().stream().map(LogEntry::getMessage).toList();
        assertThat(messages)
                .as("audit log should mention the resolved plan and target index")
                .anyMatch(m -> m.contains("plan-ok") && m.contains("idx-plan-ok"));
    }

    @Test
    void failedPlanRejectsRequestWithPlanIdInLog() {
        plans.put("plan-bad", IndexPlanCache.FetchOutcome.found(
                readyPlan("plan-bad").toBuilder()
                        .setStatus(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED)
                        .setLastError("knn provisioning failed")
                        .build()));

        ProcessDataResponse resp = processor
                .processData(requestWithConfig(configWithPlanIds("plan-bad")))
                .await().indefinitely();

        assertThat(resp.getOutcome())
                .as("FAILED plan must surface as PROCESSING_OUTCOME_FAILURE")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
        assertThat(resp.getLogEntriesList())
                .as("error log must name the offending plan id and its FAILED status")
                .anyMatch(le -> le.getMessage().contains("plan-bad")
                        && le.getMessage().contains("FAILED"));
    }

    @Test
    void missingPlanRejectsRequestWithPlanIdInLog() {
        // No plan registered → fetcher returns Missing()

        ProcessDataResponse resp = processor
                .processData(requestWithConfig(configWithPlanIds("plan-nope")))
                .await().indefinitely();

        assertThat(resp.getOutcome())
                .as("missing plan id must surface as PROCESSING_OUTCOME_FAILURE")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
        assertThat(resp.getLogEntriesList())
                .as("error log must name the missing plan id")
                .anyMatch(le -> le.getMessage().contains("plan-nope")
                        && le.getMessage().contains("missing"));
    }

    @Test
    void multiplePlansOneMissingRejectsRequestAtomically() {
        plans.put("plan-ok", IndexPlanCache.FetchOutcome.found(readyPlan("plan-ok")));
        // plan-missing not registered

        ProcessDataResponse resp = processor
                .processData(requestWithConfig(configWithPlanIds("plan-ok", "plan-missing")))
                .await().indefinitely();

        assertThat(resp.getOutcome())
                .as("any single missing plan must reject the whole request — no partial indexing")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
        assertThat(resp.getLogEntriesList())
                .as("error log must name the missing plan id even when other ids are READY")
                .anyMatch(le -> le.getMessage().contains("plan-missing"));
    }

    @Test
    void emptyPlanIdsRejectsRequest() {
        ProcessDataResponse resp = processor
                .processData(requestWithConfig(configWithPlanIds()))
                .await().indefinitely();

        assertThat(resp.getOutcome())
                .as("empty plan_ids list must reject — sink requires at least one plan reference")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE);
        assertThat(resp.getLogEntriesList())
                .as("error log must explain that plan_ids is empty / required")
                .anyMatch(le -> le.getMessage().toLowerCase().contains("plan_ids")
                        || le.getMessage().toLowerCase().contains("plan"));
    }
}
