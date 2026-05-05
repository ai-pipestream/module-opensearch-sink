package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class OpenSearchIngestionStreamBatchingTest {

    private OpenSearchIngestionServiceImpl service;
    private RecordingSchemaManager schemaManager;

    @BeforeEach
    void setUp() {
        schemaManager = new RecordingSchemaManager();
        service = new OpenSearchIngestionServiceImpl();
        service.schemaManager = schemaManager;
        service.documentConverter = new DocumentConverterService();
    }

    @Test
    void streamDocuments_opensOneManagerStreamPerMicroBatch() {
        List<StreamDocumentsRequest> requests = IntStream.range(0, 250)
                .mapToObj(i -> request("req-" + i, doc("doc-" + i, "article", "sem-article")))
                .toList();

        List<StreamDocumentsResponse> responses = service.streamDocuments(Multi.createFrom().iterable(requests))
                .collect().asList().await().indefinitely();

        assertThat(responses)
                .as("every input document should receive one correlated response")
                .hasSize(250);
        assertThat(schemaManager.streamedBatches)
                .as("250 docs should be sent as 3 manager streams: 100, 100, 50")
                .extracting(List::size)
                .containsExactly(100, 100, 50);
    }

    @Test
    void streamDocuments_provisionsEveryDistinctIndexInHeterogeneousBatch() {
        List<StreamDocumentsRequest> requests = List.of(
                request("req-article", doc("doc-article", "article", "sem-article")),
                request("req-pdf", doc("doc-pdf", "pdf", "sem-pdf")),
                request("req-article-2", doc("doc-article-2", "article", "sem-article"))
        );

        List<StreamDocumentsResponse> responses = service.streamDocuments(Multi.createFrom().iterable(requests))
                .collect().asList().await().indefinitely();

        assertThat(responses)
                .as("heterogeneous batches should still index every document")
                .hasSize(3);
        assertThat(schemaManager.provisionedKeys)
                .as("both document types in the same micro-batch should be eagerly provisioned")
                .containsExactly("pipeline-article|sem-article", "pipeline-pdf|sem-pdf");
        assertThat(schemaManager.streamedBatches)
                .as("heterogeneous provisioning should not split the manager stream")
                .hasSize(1);
    }

    @Test
    void streamDocuments_provisioningFailureFailsBatchWithoutOpeningManagerStream() {
        schemaManager.failedKeys.add("pipeline-pdf|sem-pdf");

        List<StreamDocumentsRequest> requests = List.of(
                request("req-pdf-1", doc("doc-pdf-1", "pdf", "sem-pdf")),
                request("req-pdf-2", doc("doc-pdf-2", "pdf", "sem-pdf"))
        );

        List<StreamDocumentsResponse> responses = service.streamDocuments(Multi.createFrom().iterable(requests))
                .collect().asList().await().indefinitely();

        assertThat(responses)
                .as("provisioning failure should surface per document")
                .hasSize(2)
                .allSatisfy(response -> {
                    assertThat(response.getSuccess())
                            .as("documents must not fall back to lazy bind after ProvisionIndex failure")
                            .isFalse();
                    assertThat(response.getMessage())
                            .as("failure should name provisioning rather than a generic stream error")
                            .contains("provisioning failed");
                });
        assertThat(schemaManager.streamedBatches)
                .as("manager stream must not open when provisioning failed")
                .isEmpty();
    }

    private static StreamDocumentsRequest request(String requestId, PipeDoc doc) {
        return StreamDocumentsRequest.newBuilder()
                .setRequestId(requestId)
                .setDocument(doc)
                .build();
    }

    private static PipeDoc doc(String docId, String docType, String semanticConfigId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType(docType)
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSemanticConfigId(semanticConfigId)
                                .setSourceFieldName("body")
                                .setChunkConfigId("chunker")
                                .setEmbeddingConfigId("embedder")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text for " + docId)
                                                .addVector(0.1f)
                                                .addVector(0.2f)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private static final class RecordingSchemaManager extends SchemaManagerService {
        final List<List<StreamIndexDocumentsRequest>> streamedBatches = new ArrayList<>();
        final Set<String> provisionedKeys = new LinkedHashSet<>();
        final Set<String> failedKeys = new LinkedHashSet<>();

        @Override
        public String determineIndexName(String documentType) {
            return documentType == null || documentType.isBlank()
                    ? "pipeline-documents"
                    : "pipeline-" + documentType.toLowerCase();
        }

        @Override
        public String provisioningCacheKey(String indexName, PipeDoc document) {
            String semanticIds = document.getSearchMetadata().getSemanticResultsList().stream()
                    .map(SemanticProcessingResult::getSemanticConfigId)
                    .sorted()
                    .reduce((left, right) -> left + "," + right)
                    .orElse("");
            return indexName + "|" + semanticIds;
        }

        @Override
        public Uni<Void> ensureIndexProvisioned(String indexName, PipeDoc document, String documentType) {
            String key = provisioningCacheKey(indexName, document);
            provisionedKeys.add(key);
            if (failedKeys.contains(key)) {
                return Uni.createFrom().failure(new RuntimeException("provisioning failed for " + key));
            }
            return Uni.createFrom().voidItem();
        }

        @Override
        public Multi<StreamIndexDocumentsResponse> streamIndexDocumentsViaManager(
                Multi<StreamIndexDocumentsRequest> requests) {
            return requests.collect().asList()
                    .onItem().transformToMulti(batch -> {
                        streamedBatches.add(batch);
                        return Multi.createFrom().iterable(batch)
                                .onItem().transform(req -> StreamIndexDocumentsResponse.newBuilder()
                                        .setRequestId(req.getRequestId())
                                        .setDocumentId(req.getDocumentId())
                                        .setSuccess(true)
                                        .setMessage("ok")
                                        .build());
                    });
        }
    }
}
