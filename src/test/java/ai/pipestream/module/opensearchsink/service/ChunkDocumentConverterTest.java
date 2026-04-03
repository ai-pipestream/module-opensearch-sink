package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.data.v1.*;
import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ChunkDocumentConverter — validates conversion of PipeDoc + OpenSearchDocument
 * into flat chunk documents for CHUNK_COMBINED and SEPARATE_INDICES strategies.
 */
class ChunkDocumentConverterTest {

    private ChunkDocumentConverter converter;

    @BeforeEach
    void setUp() {
        converter = new ChunkDocumentConverter();
    }

    // ---- Test helpers ----

    /**
     * Build a minimal PipeDoc with optional ownership.
     */
    private PipeDoc buildPipeDoc(String docId, boolean withOwnership) {
        PipeDoc.Builder builder = PipeDoc.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType("article")
                        .setTitle("Test Title")
                        .build());
        if (withOwnership) {
            builder.setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("acct-42")
                    .setDatasourceId("ds-7")
                    .setConnectorId("conn-99")
                    .build());
        }
        return builder.build();
    }

    /**
     * Build an OpenSearchDocument with two SemanticVectorSets sharing the same chunk_config_id
     * but different embedding_ids. Each has 2 embeddings with the same source texts.
     */
    private OpenSearchDocument buildTwoModelOsDoc() {
        return OpenSearchDocument.newBuilder()
                .setOriginalDocId("doc-1")
                .setDocType("article")
                .setTitle("Test Title")
                .setSourceUri("https://example.com/doc-1")
                .setCreatedBy("test")
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId("chunker-sentence")
                        .setEmbeddingId("model-a")
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("Hello world.")
                                .setChunkConfigId("chunker-sentence")
                                .setEmbeddingId("model-a")
                                .addVector(0.1f).addVector(0.2f)
                                .setIsPrimary(false)
                                .build())
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("Goodbye world.")
                                .setChunkConfigId("chunker-sentence")
                                .setEmbeddingId("model-a")
                                .addVector(0.3f).addVector(0.4f)
                                .setIsPrimary(false)
                                .build())
                        .build())
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId("chunker-sentence")
                        .setEmbeddingId("model-b")
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("Hello world.")
                                .setChunkConfigId("chunker-sentence")
                                .setEmbeddingId("model-b")
                                .addVector(0.5f).addVector(0.6f)
                                .setIsPrimary(false)
                                .build())
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("Goodbye world.")
                                .setChunkConfigId("chunker-sentence")
                                .setEmbeddingId("model-b")
                                .addVector(0.7f).addVector(0.8f)
                                .setIsPrimary(false)
                                .build())
                        .build())
                .build();
    }

    // ---- Tests ----

    @Test
    void test_chunkCombined_groupsByChunkConfig() {
        PipeDoc doc = buildPipeDoc("doc-1", false);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        List<OpenSearchChunkDocument> chunks = result.chunkDocuments();

        // Two source texts, same chunk config -> 2 chunk docs, each with 2 embeddings
        assertThat(chunks).as("CHUNK_COMBINED should group same source_text into one chunk doc")
                .hasSize(2);

        OpenSearchChunkDocument first = chunks.get(0);
        assertThat(first.getSourceText()).as("first chunk source text")
                .isEqualTo("Hello world.");
        assertThat(first.getEmbeddingsMap()).as("first chunk should have 2 embedding models")
                .hasSize(2);
        assertThat(first.getEmbeddingsMap()).as("first chunk should have model-a embedding")
                .containsKey("model-a");
        assertThat(first.getEmbeddingsMap()).as("first chunk should have model-b embedding")
                .containsKey("model-b");
        assertThat(first.getEmbeddingsMap().get("model-a").getValuesList())
                .as("model-a vector for first chunk")
                .containsExactly(0.1f, 0.2f);
        assertThat(first.getEmbeddingsMap().get("model-b").getValuesList())
                .as("model-b vector for first chunk")
                .containsExactly(0.5f, 0.6f);

        OpenSearchChunkDocument second = chunks.get(1);
        assertThat(second.getSourceText()).as("second chunk source text")
                .isEqualTo("Goodbye world.");
        assertThat(second.getEmbeddingsMap()).as("second chunk should have 2 embedding models")
                .hasSize(2);
        assertThat(second.getChunkIndex()).as("second chunk should have index 1")
                .isEqualTo(1);
    }

    @Test
    void test_separateIndices_oneDocPerEmbedding() {
        PipeDoc doc = buildPipeDoc("doc-1", false);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);

        List<OpenSearchChunkDocument> chunks = result.chunkDocuments();

        // 2 sets x 2 embeddings each = 4 chunk docs, each with 1 embedding
        assertThat(chunks).as("SEPARATE_INDICES should produce one doc per embedding")
                .hasSize(4);

        for (OpenSearchChunkDocument chunk : chunks) {
            assertThat(chunk.getEmbeddingsMap()).as("each separate-index chunk should have exactly 1 embedding")
                    .hasSize(1);
        }

        // Verify model-a chunks
        List<OpenSearchChunkDocument> modelAChunks = chunks.stream()
                .filter(c -> c.getEmbeddingsMap().containsKey("model-a"))
                .toList();
        assertThat(modelAChunks).as("model-a chunks").hasSize(2);

        // Verify model-b chunks
        List<OpenSearchChunkDocument> modelBChunks = chunks.stream()
                .filter(c -> c.getEmbeddingsMap().containsKey("model-b"))
                .toList();
        assertThat(modelBChunks).as("model-b chunks").hasSize(2);
    }

    @Test
    void test_documentMap_hasNoVectors() {
        PipeDoc doc = buildPipeDoc("doc-1", true);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        OpenSearchDocumentMap docMap = result.documentMap();

        // Parent fields preserved
        assertThat(docMap.getOriginalDocId()).as("document map doc id")
                .isEqualTo("doc-1");
        assertThat(docMap.getTitle()).as("document map title")
                .isEqualTo("Test Title");
        assertThat(docMap.getDocType()).as("document map doc type")
                .isEqualTo("article");
        assertThat(docMap.getSourceUri()).as("document map source uri")
                .isEqualTo("https://example.com/doc-1");

        // Chunk indices populated
        assertThat(docMap.getChunkIndicesCount()).as("document map should have chunk index references")
                .isGreaterThan(0);

        // ACL populated
        assertThat(docMap.hasAcl()).as("document map should have ACL").isTrue();
    }

    @Test
    void test_aclPopulatedFromOwnership() {
        PipeDoc doc = buildPipeDoc("doc-1", true);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        // Verify ACL on document map
        Struct mapAcl = result.documentMap().getAcl();
        assertThat(mapAcl.getFieldsMap().get("account_id").getStringValue())
                .as("document map ACL account_id")
                .isEqualTo("acct-42");
        assertThat(mapAcl.getFieldsMap().get("datasource_id").getStringValue())
                .as("document map ACL datasource_id")
                .isEqualTo("ds-7");
        assertThat(mapAcl.getFieldsMap().get("connector_id").getStringValue())
                .as("document map ACL connector_id")
                .isEqualTo("conn-99");

        // Verify ACL on chunk documents
        for (OpenSearchChunkDocument chunk : result.chunkDocuments()) {
            Struct chunkAcl = chunk.getAcl();
            assertThat(chunkAcl.getFieldsMap().get("account_id").getStringValue())
                    .as("chunk ACL account_id for chunk index " + chunk.getChunkIndex())
                    .isEqualTo("acct-42");
            assertThat(chunkAcl.getFieldsMap().get("datasource_id").getStringValue())
                    .as("chunk ACL datasource_id for chunk index " + chunk.getChunkIndex())
                    .isEqualTo("ds-7");
            assertThat(chunkAcl.getFieldsMap().get("connector_id").getStringValue())
                    .as("chunk ACL connector_id for chunk index " + chunk.getChunkIndex())
                    .isEqualTo("conn-99");
        }

        // Verify audit log mentions ACL
        assertThat(result.auditLogs()).as("audit logs should mention ACL")
                .anyMatch(msg -> msg.contains("ACL") && msg.contains("acct-42"));
    }

    @Test
    void test_chunkAnalyticsPreserved() {
        ChunkAnalytics analytics = ChunkAnalytics.newBuilder()
                .setCharacterCount(150)
                .setWordCount(25)
                .setSentenceCount(3)
                .build();

        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder()
                .setOriginalDocId("doc-analytics")
                .setDocType("article")
                .setTitle("Analytics Doc")
                .setCreatedBy("test")
                .addSemanticSets(SemanticVectorSet.newBuilder()
                        .setSourceFieldName("body")
                        .setChunkConfigId("chunker-1")
                        .setEmbeddingId("model-1")
                        .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                .setSourceText("Text with analytics.")
                                .setChunkConfigId("chunker-1")
                                .setEmbeddingId("model-1")
                                .addVector(1.0f).addVector(2.0f)
                                .setIsPrimary(false)
                                .setChunkAnalytics(analytics)
                                .build())
                        .build())
                .build();

        PipeDoc doc = buildPipeDoc("doc-analytics", false);

        // Test with CHUNK_COMBINED
        ChunkConversionResult combinedResult = converter.convertToChunks(
                doc, osDoc, "idx", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);
        assertThat(combinedResult.chunkDocuments()).as("combined chunks").hasSize(1);
        assertThat(combinedResult.chunkDocuments().get(0).hasChunkAnalytics())
                .as("combined chunk should have analytics").isTrue();
        assertThat(combinedResult.chunkDocuments().get(0).getChunkAnalytics().getWordCount())
                .as("combined chunk word count").isEqualTo(25);
        assertThat(combinedResult.chunkDocuments().get(0).getChunkAnalytics().getCharacterCount())
                .as("combined chunk character count").isEqualTo(150);
        assertThat(combinedResult.chunkDocuments().get(0).getChunkAnalytics().getSentenceCount())
                .as("combined chunk sentence count").isEqualTo(3);

        // Test with SEPARATE_INDICES
        ChunkConversionResult separateResult = converter.convertToChunks(
                doc, osDoc, "idx", IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);
        assertThat(separateResult.chunkDocuments()).as("separate chunks").hasSize(1);
        assertThat(separateResult.chunkDocuments().get(0).hasChunkAnalytics())
                .as("separate chunk should have analytics").isTrue();
        assertThat(separateResult.chunkDocuments().get(0).getChunkAnalytics().getSentenceCount())
                .as("separate chunk sentence count").isEqualTo(3);
    }

    @Test
    void test_indexNaming_chunkCombined() {
        PipeDoc doc = buildPipeDoc("doc-1", false);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED);

        OpenSearchDocumentMap docMap = result.documentMap();

        // Should have 1 chunk index reference (both sets share the same chunk_config_id)
        assertThat(docMap.getChunkIndicesCount()).as("chunk index count for CHUNK_COMBINED")
                .isEqualTo(1);

        ChunkIndexReference ref = docMap.getChunkIndices(0);
        assertThat(ref.getIndexName()).as("CHUNK_COMBINED index name")
                .isEqualTo("my-index--chunk--chunker_sentence");
        assertThat(ref.getChunkConfigId()).as("chunk config id in reference")
                .isEqualTo("chunker-sentence");
        assertThat(ref.getEmbeddingModelsList()).as("embedding models in reference")
                .containsExactlyInAnyOrder("model-a", "model-b");
    }

    @Test
    void test_indexNaming_separateIndices() {
        PipeDoc doc = buildPipeDoc("doc-1", false);
        OpenSearchDocument osDoc = buildTwoModelOsDoc();

        ChunkConversionResult result = converter.convertToChunks(
                doc, osDoc, "my-index", IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES);

        OpenSearchDocumentMap docMap = result.documentMap();

        // Should have 2 chunk index references (one per embedding model)
        assertThat(docMap.getChunkIndicesCount()).as("chunk index count for SEPARATE_INDICES")
                .isEqualTo(2);

        List<String> indexNames = docMap.getChunkIndicesList().stream()
                .map(ChunkIndexReference::getIndexName)
                .toList();

        assertThat(indexNames).as("SEPARATE_INDICES index names")
                .containsExactlyInAnyOrder(
                        "my-index--vs--chunker_sentence--model_a",
                        "my-index--vs--chunker_sentence--model_b"
                );
    }
}
