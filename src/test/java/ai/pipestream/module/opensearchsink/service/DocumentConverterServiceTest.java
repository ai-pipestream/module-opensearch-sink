package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.data.v1.*;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class DocumentConverterServiceTest {

    private DocumentConverterService converter;

    @BeforeEach
    void setUp() {
        converter = new DocumentConverterService();
    }

    @Test
    void testConvertToOpenSearchDocument() {
        // 1. Setup a complex PipeDoc
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
                        // Add a semantic result
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("chunker-1")
                                .setEmbeddingConfigId("model-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("Test Body Content")
                                                .addVector(0.1f)
                                                .addVector(0.2f)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        // 2. Convert
        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        // 3. Assert core fields
        assertThat(osDoc.getOriginalDocId(), equalTo("test-id-123"));
        assertThat(osDoc.getTitle(), equalTo("Test Title"));
        assertThat(osDoc.getBody(), equalTo("Test Body Content"));
        assertThat(osDoc.getDocType(), equalTo("article"));
        assertThat(osDoc.getTagsList(), containsInAnyOrder("java", "quarkus"));

        // 4. Assert Semantic Sets
        assertThat(osDoc.getSemanticSetsCount(), equalTo(1));
        SemanticVectorSet vset = osDoc.getSemanticSets(0);
        assertThat(vset.getSourceFieldName(), equalTo("body"));
        assertThat(vset.getChunkConfigId(), equalTo("chunker-1"));
        assertThat(vset.getEmbeddingId(), equalTo("model-1"));
        assertThat(vset.getEmbeddingsCount(), equalTo(1));
        assertThat(vset.getEmbeddings(0).getVectorList(), contains(0.1f, 0.2f));
        assertThat(vset.getEmbeddings(0).getSourceText(), equalTo("Test Body Content"));
    }

    @Test
    void testDeduplicationBySourceText() {
        // Setup result with duplicate text chunks (common in sliding windows or overlaps)
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

        assertThat(osDoc.getSemanticSets(0).getEmbeddingsCount(), equalTo(2));
    }
}
