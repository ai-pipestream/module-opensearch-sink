package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.data.v1.*;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for DocumentConverterService — validates PipeDoc → OpenSearchDocument conversion
 * including semantic result sets, field-level embeddings, deduplication, and edge cases.
 */
class DocumentConverterServiceTest {

    private DocumentConverterService converter;

    @BeforeEach
    void setUp() {
        converter = new DocumentConverterService();
    }

    @Test
    void convertToOpenSearchDocument_coreFields() {
        Timestamp now = Timestamp.newBuilder().setSeconds(1710000000).build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-id-123")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setDocumentType("article")
                        .setTitle("Test Title")
                        .setBody("Test Body Content")
                        .setCreationDate(now)
                        .setLastModifiedDate(now)
                        .setKeywords(Keywords.newBuilder().addKeyword("java").addKeyword("quarkus").build())
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("chunker-1")
                                .setEmbeddingConfigId("model-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("Test Body Content")
                                                .addVector(0.1f).addVector(0.2f)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getOriginalDocId()).as("doc id").isEqualTo("test-id-123");
        assertThat(osDoc.getTitle()).as("title").isEqualTo("Test Title");
        assertThat(osDoc.getBody()).as("body").isEqualTo("Test Body Content");
        assertThat(osDoc.getDocType()).as("doc type").isEqualTo("article");
        assertThat(osDoc.getTagsList()).as("tags").containsExactlyInAnyOrder("java", "quarkus");
        assertThat(osDoc.getSemanticSetsCount()).as("semantic set count").isEqualTo(1);

        SemanticVectorSet vset = osDoc.getSemanticSets(0);
        assertThat(vset.getSourceFieldName()).as("source field").isEqualTo("body");
        assertThat(vset.getChunkConfigId()).as("chunk config id").isEqualTo("chunker-1");
        assertThat(vset.getEmbeddingId()).as("embedding id").isEqualTo("model-1");
        assertThat(vset.getEmbeddingsCount()).as("embedding count").isEqualTo(1);
        assertThat(vset.getEmbeddings(0).getVectorList()).as("vector values").containsExactly(0.1f, 0.2f);
        assertThat(vset.getEmbeddings(0).getSourceText()).as("source text").isEqualTo("Test Body Content");
    }

    @Test
    void deduplicationBySourceText() {
        SemanticProcessingResult result = SemanticProcessingResult.newBuilder()
                .setSourceFieldName("body")
                .setChunkConfigId("c1")
                .setEmbeddingConfigId("m1")
                .addChunks(SemanticChunk.newBuilder()
                        .setEmbeddingInfo(ChunkEmbedding.newBuilder().setTextContent("duplicate").addVector(1.0f).build()))
                .addChunks(SemanticChunk.newBuilder()
                        .setEmbeddingInfo(ChunkEmbedding.newBuilder().setTextContent("duplicate").addVector(1.0f).build()))
                .addChunks(SemanticChunk.newBuilder()
                        .setEmbeddingInfo(ChunkEmbedding.newBuilder().setTextContent("unique").addVector(2.0f).build()))
                .build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("id")
                .setSearchMetadata(SearchMetadata.newBuilder().addSemanticResults(result).build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getSemanticSets(0).getEmbeddingsCount())
                .as("2 unique texts after dedup (3 chunks → 2 embeddings)")
                .isEqualTo(2);
    }

    @Test
    void fieldLevelEmbedding_singleChunkPerResult() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Full body text here.")
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("field_level")
                                .setEmbeddingConfigId("all-MiniLM-L6-v2")
                                .setResultSetName("body_field_level_all-MiniLM-L6-v2")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("doc1_body_full")
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("Full body text here.")
                                                .addVector(0.5f).addVector(0.6f)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getSemanticSetsCount()).as("single semantic set for field-level").isEqualTo(1);

        SemanticVectorSet vset = osDoc.getSemanticSets(0);
        assertThat(vset.getChunkConfigId()).as("chunk_config_id = field_level").isEqualTo("field_level");
        assertThat(vset.getEmbeddingId()).as("embedding model").isEqualTo("all-MiniLM-L6-v2");
        assertThat(vset.getEmbeddingsCount()).as("single embedding for whole field").isEqualTo(1);
        assertThat(vset.getEmbeddings(0).getSourceText()).as("source text = full field")
                .isEqualTo("Full body text here.");
        assertThat(vset.getEmbeddings(0).getVectorList()).as("vector").containsExactly(0.5f, 0.6f);
    }

    @Test
    void multipleSemanticSets_distinctTriples_noClash() {
        // Same source field with different (chunk_config_id, embedding_config_id) combinations
        // must produce separate SemanticVectorSets with no merging
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("multi")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(buildResult("body", "token_500", "all-MiniLM-L6-v2", "chunk 1", 0.1f))
                        .addSemanticResults(buildResult("body", "field_level", "all-MiniLM-L6-v2", "full body", 0.2f))
                        .addSemanticResults(buildResult("body", "token_500", "e5-small-v2", "chunk 1", 0.3f))
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getSemanticSetsCount())
                .as("3 unique (source, chunk_config, embedding) triples → 3 sets")
                .isEqualTo(3);

        var triples = osDoc.getSemanticSetsList().stream()
                .map(vs -> vs.getSourceFieldName() + "|" + vs.getChunkConfigId() + "|" + vs.getEmbeddingId())
                .toList();
        assertThat(triples).as("all three unique triples present")
                .containsExactlyInAnyOrder(
                        "body|token_500|all-MiniLM-L6-v2",
                        "body|field_level|all-MiniLM-L6-v2",
                        "body|token_500|e5-small-v2");
    }

    @Test
    void skipsResultsWithMissingChunkConfigId() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("id")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                // no chunk_config_id
                                .setEmbeddingConfigId("model-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.1f).build()))
                                .build())
                        .addSemanticResults(buildResult("body", "c1", "m1", "text", 0.1f))
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);

        assertThat(result.document().getSemanticSetsCount())
                .as("only valid result passes through").isEqualTo(1);
        assertThat(result.auditLogs()).as("audit log mentions skipped set")
                .anyMatch(l -> l.contains("Skipping"));
    }

    @Test
    void skipsResultsWithMissingEmbeddingConfigId() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("id")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                // no embedding_config_id
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.1f).build()))
                                .build())
                        .addSemanticResults(buildResult("body", "c1", "m1", "text", 0.1f))
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);

        assertThat(result.document().getSemanticSetsCount())
                .as("only valid result passes through").isEqualTo(1);
        assertThat(result.auditLogs()).as("audit log mentions skipped set")
                .anyMatch(l -> l.contains("Skipping"));
    }

    @Test
    void chunksWithNoVectors_excludedWithAuditWarning() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("id")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                .setEmbeddingConfigId("m1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("has vector").addVector(0.1f).build()))
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("no vector").build()))
                                .build())
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);

        assertThat(result.document().getSemanticSets(0).getEmbeddingsCount())
                .as("only chunk with vector included").isEqualTo(1);
        assertThat(result.auditLogs()).as("audit log warns about vectorless chunks")
                .anyMatch(l -> l.contains("no vectors"));
    }

    @Test
    void noSemanticResults_producesEmptySemanticSets() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("plain-doc")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setBody("Just text, no embeddings")
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getSemanticSetsCount()).as("no semantic sets for plain doc").isZero();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static SemanticProcessingResult buildResult(String sourceField, String chunkConfigId,
                                                         String embeddingConfigId, String text, float vector) {
        return SemanticProcessingResult.newBuilder()
                .setSourceFieldName(sourceField)
                .setChunkConfigId(chunkConfigId)
                .setEmbeddingConfigId(embeddingConfigId)
                .addChunks(SemanticChunk.newBuilder()
                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                .setTextContent(text).addVector(vector).build())
                        .build())
                .build();
    }
}
