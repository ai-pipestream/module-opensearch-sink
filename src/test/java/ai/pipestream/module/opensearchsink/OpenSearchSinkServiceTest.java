package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.model.ChunkEmbedding;
import ai.pipestream.data.model.PipeDoc;
import ai.pipestream.data.model.SemanticChunk;
import ai.pipestream.data.model.SemanticProcessingResult;
import ai.pipestream.ingestion.proto.IngestionRequest;
import ai.pipestream.ingestion.proto.IngestionResponse;
import ai.pipestream.module.opensearchsink.util.OpenSearchTestClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class OpenSearchSinkServiceTest {

    @Inject
    OpenSearchIngestionServiceImpl ingestionService;

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
                .setId("doc-123")
                .setDocumentType("test-doc")
                .addSemanticResults(SemanticProcessingResult.newBuilder()
                        .addChunks(SemanticChunk.newBuilder()
                                .setChunkId("chunk-abc")
                                .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                        .setTextContent("This is a test chunk.")
                                        .addVector(1.0f).addVector(2.0f).addVector(3.0f)
                                )
                        )
                ).build();

        IngestionRequest request = IngestionRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        // 2. Stream the request to the service
        List<IngestionResponse> responses = ingestionService.streamDocuments(Multi.createFrom().item(request))
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
