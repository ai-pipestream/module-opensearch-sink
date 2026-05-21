package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.module.opensearchsink.plan.IndexPlanCache;
import ai.pipestream.module.opensearchsink.support.PipeStreamTestSupport;
import ai.pipestream.module.opensearchsink.work.OpenSearchModuleProcessor;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.server.work.ModuleProcessor;
import ai.pipestream.test.support.OpenSearchSinkWireMockTestResource;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

/**
 * Service-level tests for the IndexPlan refactor on the demand-pull processor.
 */
@QuarkusTest
@QuarkusTestResource(OpenSearchSinkWireMockTestResource.class)
public class OpenSearchSinkPlanResolutionTest {

    @Inject
    OpenSearchModuleProcessor processor;

    @Inject
    IndexPlanCache planCache;

    @InjectMock
    OpenSearchIndexingPublisher indexingPublisher;

    private Map<String, IndexPlanCache.FetchOutcome> plans;

    @BeforeEach
    void seedPlans() {
        plans = new HashMap<>();
        planCache.setFetcher(id -> {
            IndexPlanCache.FetchOutcome o = plans.get(id);
            return o == null ? IndexPlanCache.FetchOutcome.missing() : o;
        });
        planCache.invalidateAll();

        Mockito.when(indexingPublisher.publish(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString()))
                .thenReturn("0-0");
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

    private static PipeDoc testDoc() {
        return PipeDoc.newBuilder()
                .setDocId("doc-plan-test")
                .setSearchMetadata(SearchMetadata.newBuilder().setDocumentType("any-type").build())
                .build();
    }

    @Test
    void readyPlanResolvesAndQueuesForIndexing() throws Exception {
        plans.put("plan-ok", IndexPlanCache.FetchOutcome.found(readyPlan("plan-ok")));

        PipeStream result = processor.process(
                PipeStreamTestSupport.withSinkConfig(testDoc(), "test-cluster", "plan-ok"));

        assertThat(result.getDocument().getDocId()).isEqualTo("doc-plan-test");
        verify(indexingPublisher).publish(
                Mockito.argThat(plan -> "plan-ok".equals(plan.id())),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString());
    }

    @Test
    void failedPlanRejectsWithPermanentFailure() throws Exception {
        plans.put("plan-bad", IndexPlanCache.FetchOutcome.found(
                readyPlan("plan-bad").toBuilder()
                        .setStatus(IndexPlanStatus.INDEX_PLAN_STATUS_FAILED)
                        .setLastError("knn provisioning failed")
                        .build()));

        assertThatThrownBy(() -> processor.process(
                PipeStreamTestSupport.withSinkConfig(testDoc(), "test-cluster", "plan-bad")))
                .isInstanceOf(ModuleProcessor.PermanentFailure.class)
                .hasMessageContaining("plan-bad")
                .hasMessageContaining("FAILED");
    }

    @Test
    void missingPlanRejectsWithPermanentFailure() throws Exception {
        assertThatThrownBy(() -> processor.process(
                PipeStreamTestSupport.withSinkConfig(testDoc(), "test-cluster", "plan-nope")))
                .isInstanceOf(ModuleProcessor.PermanentFailure.class)
                .hasMessageContaining("plan-nope")
                .hasMessageContaining("missing");
    }

    @Test
    void multiplePlansOneMissingRejectsAtomically() throws Exception {
        plans.put("plan-ok", IndexPlanCache.FetchOutcome.found(readyPlan("plan-ok")));

        assertThatThrownBy(() -> processor.process(
                PipeStreamTestSupport.withSinkConfig(testDoc(), "test-cluster", "plan-ok", "plan-missing")))
                .isInstanceOf(ModuleProcessor.PermanentFailure.class)
                .hasMessageContaining("plan-missing");
    }

    @Test
    void emptyPlanIdsRejectsWithPermanentFailure() throws Exception {
        assertThatThrownBy(() -> processor.process(
                PipeStreamTestSupport.withSinkConfig(testDoc(), "test-cluster")))
                .isInstanceOf(ModuleProcessor.PermanentFailure.class)
                .satisfiesAnyOf(
                        ex -> assertThat(ex.getMessage().toLowerCase()).contains("plan_ids"),
                        ex -> assertThat(ex.getMessage().toLowerCase()).contains("plan"));
    }
}
