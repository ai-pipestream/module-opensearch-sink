package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.Timestamp;
import ai.pipestream.data.v1.*;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.test.support.OpenSearchSinkWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(OpenSearchSinkWireMockTestResource.class)
public class OpenSearchSinkServiceTest {

    @GrpcClient("opensearchSink")
    MutinyOpenSearchIngestionServiceGrpc.MutinyOpenSearchIngestionServiceStub ingestionClient;

    @Inject
    @io.quarkus.grpc.GrpcService
    OpenSearchIngestionServiceImpl processorService;

    @Test
    void testStreamDocuments_MissingConfig_FailsEarly() {
        // Without config, the streaming path should fail early indicating missing index name
        long now = System.currentTimeMillis() / 1000;
        PipeDoc testDoc = createTestDoc("doc-123", "test-doc", now);

        StreamDocumentsRequest request = StreamDocumentsRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        List<StreamDocumentsResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
                .collect().asList().await().indefinitely();

        assertEquals(1, responses.size());
        assertFalse(responses.get(0).getSuccess());
        assertTrue(responses.get(0).getMessage().contains("Missing target index name"));
    }

    @Test
    void testProcessData_CustomConfig_RoutingToSpecificIndex() {
        // This test simulates the engine passing a custom JSON configuration
        long now = System.currentTimeMillis() / 1000;
        PipeDoc testDoc = createTestDoc("doc-custom-cfg", "any-type", now);

        // Build the Struct that JSONForms would produce
        Struct jsonConfig = Struct.newBuilder()
                .putFields("opensearch_instance", Value.newBuilder().setStringValue("prod-cluster").build())
                .putFields("index_name", Value.newBuilder().setStringValue("manual-override-index").build())
                .putFields("indexing_strategy", Value.newBuilder().setStringValue("NESTED").build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(jsonConfig)
                        .build())
                .build();

        ProcessDataResponse response = processorService.processData(request).await().indefinitely();

        assertTrue(response.getSuccess());
        // Verify we got the success message from the mock (which means validation passed)
        assertTrue(response.getProcessorLogs(0).contains("WireMock"));
    }

    @Test
    void testProcessData_ForcedInternalError() {
        // Trigger error by using the "fail-this-index" trigger which our high-fidelity mock recognizes
        PipeDoc testDoc = createTestDoc("doc-fail", "any-type", System.currentTimeMillis() / 1000);

        Struct jsonConfig = Struct.newBuilder()
                .putFields("index_name", Value.newBuilder().setStringValue("fail-this-index").build())
                .build();

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(testDoc)
                .setConfig(ProcessConfiguration.newBuilder()
                        .setJsonConfig(jsonConfig)
                        .build())
                .build();

        ProcessDataResponse response = processorService.processData(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertTrue(response.getProcessorLogs(0).contains("Forced internal error"));
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
