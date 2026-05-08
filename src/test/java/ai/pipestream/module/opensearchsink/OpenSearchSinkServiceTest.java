package ai.pipestream.module.opensearchsink;

import com.google.protobuf.Timestamp;
import ai.pipestream.data.v1.*;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.module.opensearchsink.plan.IndexPlanCache;
import ai.pipestream.opensearch.v1.IndexPlan;
import ai.pipestream.opensearch.v1.IndexPlanStatus;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.test.support.OpenSearchSinkWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(OpenSearchSinkWireMockTestResource.class)
public class OpenSearchSinkServiceTest {

    @GrpcClient("opensearchSink")
    MutinyOpenSearchIngestionServiceGrpc.MutinyOpenSearchIngestionServiceStub ingestionClient;

    @GrpcClient("opensearchSink")
    ai.pipestream.data.module.v1.MutinyPipeStepProcessorServiceGrpc.MutinyPipeStepProcessorServiceStub processorClient;

    @Inject
    SchemaManagerService schemaManager;

    @Inject
    IndexPlanCache planCache;

    private Map<String, IndexPlanCache.FetchOutcome> plans;

    @BeforeEach
    void seedPlans() {
        plans = new HashMap<>();
        plans.put("plan-manual-override", IndexPlanCache.FetchOutcome.found(
                IndexPlan.newBuilder()
                        .setId("plan-manual-override")
                        .setName("plan-manual-override")
                        .setIndexName("manual-override-index")
                        .setIndexingStrategy(IndexingStrategy.INDEXING_STRATEGY_NESTED)
                        .setStatus(IndexPlanStatus.INDEX_PLAN_STATUS_READY)
                        .build()));
        planCache.setFetcher(id -> {
            IndexPlanCache.FetchOutcome o = plans.get(id);
            return o == null ? IndexPlanCache.FetchOutcome.missing() : o;
        });
        planCache.invalidateAll();
    }

    @Test
    void testStreamDocuments_Success_ContractValidation() {
        // This test verifies that the Sink correctly formats and sends documents to the platform.
        // It relies on the High-Fidelity WireMock which validates the Protobuf contract.
        long now = System.currentTimeMillis() / 1000;
        PipeDoc testDoc = createTestDoc("doc-123", "test-doc", now);

        StreamDocumentsRequest request = StreamDocumentsRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        List<StreamDocumentsResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
                .collect().asList().await().indefinitely();

        assertEquals(1, responses.size());
        assertTrue(responses.get(0).getSuccess());
        // Verify we got the expected message from our High-Fidelity mock
        assertTrue(responses.get(0).getMessage().contains("WireMock"));
    }

    @Test
    void testStreamDocuments_MultipleSemanticSets_ContractValidation() {
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId("doc-multi-set")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType("complex-doc")
                        .addSemanticResults(createSemanticResult("body", "chunker-1", "embed-1"))
                        .addSemanticResults(createSemanticResult("summary", "chunker-2", "embed-2"))
                        .build())
                .build();

        StreamDocumentsRequest request = StreamDocumentsRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        List<StreamDocumentsResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
                .collect().asList().await().indefinitely();

        assertTrue(responses.get(0).getSuccess());
    }

    @Test
    void testStreamDocuments_ForcedInternalError() {
        // Trigger error by using the "fail-this-index" trigger which our high-fidelity mock recognizes
        PipeDoc testDoc = createTestDoc("doc-fail", "fail-this-index", System.currentTimeMillis() / 1000);

        StreamDocumentsRequest request = StreamDocumentsRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        List<StreamDocumentsResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
                .collect().asList().await().indefinitely();

        assertEquals(1, responses.size());
        assertFalse(responses.get(0).getSuccess());
        assertTrue(responses.get(0).getMessage().contains("Indexing failed"));
    }

    @Test
    void testProcessData_CustomConfig_RoutingToSpecificIndex() {
        // This test simulates the engine passing a custom JSON configuration
        long now = System.currentTimeMillis() / 1000;
        PipeDoc testDoc = createTestDoc("doc-custom-cfg", "any-type", now);

        // Build the Struct that JSONForms would produce. The sink config now
        // references an IndexPlan id; the test seeds the IndexPlanCache with
        // a READY plan whose index_name matches the manual-override-index
        // string the WireMock manager mock expects.
        com.google.protobuf.ListValue planIds = com.google.protobuf.ListValue.newBuilder()
                .addValues(com.google.protobuf.Value.newBuilder().setStringValue("plan-manual-override").build())
                .build();
        com.google.protobuf.Struct jsonConfig = com.google.protobuf.Struct.newBuilder()
                .putFields("opensearch_instance", com.google.protobuf.Value.newBuilder().setStringValue("prod-cluster").build())
                .putFields("plan_ids", com.google.protobuf.Value.newBuilder().setListValue(planIds).build())
                .build();

        ai.pipestream.data.module.v1.ProcessDataRequest request = ai.pipestream.data.module.v1.ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setConfig(ai.pipestream.data.v1.ProcessConfiguration.newBuilder()
                        .setJsonConfig(jsonConfig)
                        .build())
                .build();

        ai.pipestream.data.module.v1.ProcessDataResponse response = processorClient.processData(request).await().indefinitely();

        assertEquals(ai.pipestream.data.module.v1.ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS, response.getOutcome());
        assertTrue(response.getLogEntriesList().stream().map(LogEntry::getMessage).anyMatch(log -> log.contains("WireMock") || log.contains("indexed")));
    }

    private PipeDoc createTestDoc(String docId, String docType, long timestamp) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType(docType)
                        .setLastModifiedDate(Timestamp.newBuilder().setSeconds(timestamp).build())
                        .addSemanticResults(createSemanticResult("body", "chunker-v1", "embed-v1"))
                        .build())
                .build();
    }

    private SemanticProcessingResult createSemanticResult(String field, String chunker, String embedder) {
        return SemanticProcessingResult.newBuilder()
                .setSourceFieldName(field)
                .setChunkConfigId(chunker)
                .setEmbeddingConfigId(embedder)
                .addChunks(SemanticChunk.newBuilder()
                        .setChunkId(UUID.randomUUID().toString())
                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                .addVector(1.0f).addVector(2.0f).addVector(3.0f)
                                .build())
                        .build())
                .build();
    }
}
