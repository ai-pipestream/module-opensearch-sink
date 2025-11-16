package ai.pipestream.module.opensearchsink;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ai.pipestream.data.model.*;
import ai.pipestream.ingestion.proto.IngestionRequest;
import ai.pipestream.ingestion.proto.IngestionResponse;
import ai.pipestream.ingestion.proto.MutinyOpenSearchIngestionGrpc;
import ai.pipestream.module.opensearchsink.util.OpenSearchTestClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusIntegrationTest
public class OpenSearchSinkServiceIT {

    private ManagedChannel channel;
    private MutinyOpenSearchIngestionGrpc.MutinyOpenSearchIngestionStub client;
    private OpenSearchTestClient testClient;
    private final String indexName = "pipeline-test-doc"; // Hardcoded for simplicity

    @BeforeEach
    void setUp() {
        // The application is running in a container, but the gRPC port is mapped to the host.
        // We connect to the mapped port.
        channel = ManagedChannelBuilder.forAddress("localhost", 39104).usePlaintext().build();
        client = MutinyOpenSearchIngestionGrpc.newMutinyStub(channel);
        testClient = new OpenSearchTestClient("localhost", 9200);
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException {
        testClient.deleteIndex(indexName);
        testClient.close();
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void testStreamDocuments() throws IOException {
        // 1. Prepare the test data
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setId("doc-456")
                .setDocumentType("test-doc")
                .addSemanticResults(SemanticProcessingResult.newBuilder()
                        .addChunks(SemanticChunk.newBuilder()
                                .setChunkId("chunk-def")
                                .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                        .setTextContent("This is an integration test chunk.")
                                        .addVector(4.0f).addVector(5.0f).addVector(6.0f)
                                )
                        )
                ).build();

        IngestionRequest request = IngestionRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        // 2. Stream the request to the service
        List<IngestionResponse> responses = client.streamDocuments(Multi.createFrom().item(request))
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
