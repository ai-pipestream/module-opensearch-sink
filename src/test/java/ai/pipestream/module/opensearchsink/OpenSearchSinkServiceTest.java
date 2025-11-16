package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.ingestion.proto.IngestionRequest;
import ai.pipestream.ingestion.proto.IngestionResponse;
import ai.pipestream.ingestion.proto.MutinyOpenSearchIngestionGrpc;
import ai.pipestream.module.opensearchsink.util.OpenSearchTestClient;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
@QuarkusTest
public class OpenSearchSinkServiceTest {

    @GrpcClient("opensearchSink")
    MutinyOpenSearchIngestionGrpc.MutinyOpenSearchIngestionStub ingestionClient;

    @Inject
    SchemaManagerService schemaManager;

    private OpenSearchTestClient testClient;
    private String indexName;

    @BeforeEach
    void setUp() {
        // The test container port is mapped to a random port, but inside the container it's 9200.
        // The application connects to the container via the dev service name.
        // The test client connects via localhost and the mapped port.
        // For simplicity in this test, we assume the default port 9200 is used, as it is in the docker-compose.
        testClient = new OpenSearchTestClient("localhost", 9200);
        indexName = schemaManager.determineIndexName("test-doc");
    }

    @AfterEach
    void tearDown() throws IOException {
        testClient.deleteIndex(indexName);
        testClient.close();
    }

    @Test
    void testStreamDocuments() throws IOException {
        // 1. Prepare the test data
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId("doc-123")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType("test-doc")
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setChunkConfigId("test-chunker-v1")
                                .setEmbeddingConfigId("test-embedder-v1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("chunk-abc")
                                        .setChunkNumber(1)
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("This is a test chunk.")
                                                .setChunkId("chunk-abc")
                                                .addVector(1.0f).addVector(2.0f).addVector(3.0f)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        IngestionRequest request = IngestionRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        // 2. Stream the request to the service via the Mutiny stub client
        List<IngestionResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
                .collect().asList().await().indefinitely();

        // 3. Assert the response
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).getSuccess());

        // 4. Verify the result in OpenSearch
        testClient.refreshIndex(indexName);
        long docCount = testClient.countDocuments(indexName);
        assertEquals(1, docCount, "Should have indexed one document");
    }
}
