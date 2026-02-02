package ai.pipestream.module.opensearchsink;

import com.google.protobuf.Timestamp;
import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.module.opensearchsink.util.OpenSearchTestClient;
import ai.pipestream.test.support.OpenSearchSinkWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(OpenSearchSinkWireMockTestResource.class)
public class OpenSearchSinkServiceTest {

    @GrpcClient("opensearchSink")
    MutinyOpenSearchIngestionServiceGrpc.MutinyOpenSearchIngestionServiceStub ingestionClient;

    @Inject
    SchemaManagerService schemaManager;

    @ConfigProperty(name = "opensearch.hosts", defaultValue = "localhost:9200")
    String opensearchHosts;

    private OpenSearchTestClient testClient;
    private String indexName;

    @BeforeEach
    void setUp() {
        String[] hostPort = opensearchHosts.split(":", 2);
        String host = hostPort.length > 0 ? hostPort[0] : "localhost";
        int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 9200;
        testClient = new OpenSearchTestClient(host, port);
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
        long now = System.currentTimeMillis() / 1000;
        PipeDoc testDoc = PipeDoc.newBuilder()
                .setDocId("doc-123")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType("test-doc")
                        .setLastModifiedDate(Timestamp.newBuilder().setSeconds(now).build())
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

        StreamDocumentsRequest request = StreamDocumentsRequest.newBuilder()
                .setDocument(testDoc)
                .setRequestId(UUID.randomUUID().toString())
                .build();

        // 2. Stream the request to the service via the Mutiny stub client
        List<StreamDocumentsResponse> responses = ingestionClient.streamDocuments(Multi.createFrom().item(request))
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
