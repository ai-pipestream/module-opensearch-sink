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
    // Analytics mapping tests
    // =========================================================================

    @Test
    void sourceFieldAnalytics_transferredToOpenSearchDocument() {
        SourceFieldAnalytics bodyAnalytics = SourceFieldAnalytics.newBuilder()
                .setSourceField("body")
                .setChunkConfigId("token_500")
                .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                        .setWordCount(150)
                        .setSentenceCount(8)
                        .setDetectedLanguage("eng")
                        .setLanguageConfidence(0.98f)
                        .setNounDensity(0.28f)
                        .setVerbDensity(0.18f)
                        .setLexicalDensity(0.52f)
                        .build())
                .setTotalChunks(3)
                .setAverageChunkSize(200.0f)
                .setMinChunkSize(150)
                .setMaxChunkSize(280)
                .build();

        SourceFieldAnalytics titleAnalytics = SourceFieldAnalytics.newBuilder()
                .setSourceField("title")
                .setChunkConfigId("field_level")
                .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                        .setWordCount(6)
                        .setSentenceCount(1)
                        .setDetectedLanguage("eng")
                        .build())
                .setTotalChunks(1)
                .build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("analytics-doc")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSourceFieldAnalytics(bodyAnalytics)
                        .addSourceFieldAnalytics(titleAnalytics)
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);
        OpenSearchDocument osDoc = result.document();

        assertThat(osDoc.getSourceFieldAnalyticsCount())
                .as("both source field analytics entries transferred")
                .isEqualTo(2);

        SourceFieldAnalytics bodyResult = osDoc.getSourceFieldAnalytics(0);
        assertThat(bodyResult.getSourceField()).as("first analytics source field").isEqualTo("body");
        assertThat(bodyResult.getDocumentAnalytics().getWordCount()).as("body word count").isEqualTo(150);
        assertThat(bodyResult.getDocumentAnalytics().getDetectedLanguage()).as("body language").isEqualTo("eng");
        assertThat(bodyResult.getDocumentAnalytics().getNounDensity()).as("body noun density").isEqualTo(0.28f);
        assertThat(bodyResult.getTotalChunks()).as("body total chunks").isEqualTo(3);
        assertThat(bodyResult.getAverageChunkSize()).as("body avg chunk size").isEqualTo(200.0f);

        SourceFieldAnalytics titleResult = osDoc.getSourceFieldAnalytics(1);
        assertThat(titleResult.getSourceField()).as("second analytics source field").isEqualTo("title");
        assertThat(titleResult.getDocumentAnalytics().getWordCount()).as("title word count").isEqualTo(6);

        assertThat(result.auditLogs())
                .as("audit log mentions source field analytics")
                .anyMatch(l -> l.contains("2 source field analytics"));
    }

    @Test
    void nlpAnalysis_transferredFromFirstSemanticResult() {
        NlpDocumentAnalysis nlpAnalysis = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage("eng")
                .setLanguageConfidence(0.97f)
                .setTotalTokens(150)
                .setNounDensity(0.28f)
                .setVerbDensity(0.18f)
                .setAdjectiveDensity(0.09f)
                .setAdverbDensity(0.05f)
                .setContentWordRatio(0.60f)
                .setUniqueLemmaCount(85)
                .setLexicalDensity(0.52f)
                .addSentences(SentenceSpan.newBuilder()
                        .setText("First sentence.").setStartOffset(0).setEndOffset(15).build())
                .addSentences(SentenceSpan.newBuilder()
                        .setText("Second sentence.").setStartOffset(16).setEndOffset(32).build())
                .build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("nlp-doc")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                .setEmbeddingConfigId("m1")
                                .setNlpAnalysis(nlpAnalysis)
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.1f).build()))
                                .build())
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                .setEmbeddingConfigId("m2")
                                .setNlpAnalysis(NlpDocumentAnalysis.newBuilder()
                                        .setDetectedLanguage("deu")
                                        .build())
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.2f).build()))
                                .build())
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);
        OpenSearchDocument osDoc = result.document();

        assertThat(osDoc.hasNlpAnalysis())
                .as("NLP analysis present on OpenSearch document")
                .isTrue();

        NlpDocumentAnalysis mapped = osDoc.getNlpAnalysis();
        assertThat(mapped.getDetectedLanguage()).as("language from first result").isEqualTo("eng");
        assertThat(mapped.getTotalTokens()).as("total tokens").isEqualTo(150);
        assertThat(mapped.getNounDensity()).as("noun density").isEqualTo(0.28f);
        assertThat(mapped.getVerbDensity()).as("verb density").isEqualTo(0.18f);
        assertThat(mapped.getLexicalDensity()).as("lexical density").isEqualTo(0.52f);
        assertThat(mapped.getSentencesCount()).as("sentence count").isEqualTo(2);
        assertThat(mapped.getSentences(0).getText()).as("first sentence text").isEqualTo("First sentence.");

        assertThat(result.auditLogs())
                .as("audit log mentions NLP analysis")
                .anyMatch(l -> l.contains("NLP analysis") && l.contains("eng"));
    }

    @Test
    void nlpAnalysis_skippedWhenNoResultHasIt() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("no-nlp")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                .setEmbeddingConfigId("m1")
                                // no nlp_analysis
                                .addChunks(SemanticChunk.newBuilder()
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.1f).build()))
                                .build())
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.hasNlpAnalysis())
                .as("no NLP analysis when none present in results")
                .isFalse();
    }

    @Test
    void chunkAnalytics_transferredToOpenSearchEmbeddings() {
        ChunkAnalytics chunkAnalytics = ChunkAnalytics.newBuilder()
                .setWordCount(45)
                .setCharacterCount(280)
                .setSentenceCount(2)
                .setAverageWordLength(5.2f)
                .setNounDensity(0.30f)
                .setVerbDensity(0.15f)
                .setLexicalDensity(0.55f)
                .setRelativePosition(0.35f)
                .setIsFirstChunk(false)
                .setIsLastChunk(false)
                .setPotentialHeadingScore(0.1f)
                .build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("chunk-analytics-doc")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("c1")
                                .setEmbeddingConfigId("m1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkAnalytics(chunkAnalytics)
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("chunk text").addVector(0.1f).build()))
                                .addChunks(SemanticChunk.newBuilder()
                                        // no chunk_analytics on this one
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("plain chunk").addVector(0.2f).build()))
                                .build())
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        assertThat(osDoc.getSemanticSets(0).getEmbeddingsCount())
                .as("both chunks converted").isEqualTo(2);

        // Find the embedding with chunk_analytics (order may vary due to HashMap)
        var withAnalytics = osDoc.getSemanticSets(0).getEmbeddingsList().stream()
                .filter(e -> e.hasChunkAnalytics())
                .findFirst();
        assertThat(withAnalytics).as("embedding with chunk_analytics present").isPresent();

        ChunkAnalytics mapped = withAnalytics.get().getChunkAnalytics();
        assertThat(mapped.getWordCount()).as("chunk word count").isEqualTo(45);
        assertThat(mapped.getSentenceCount()).as("chunk sentence count").isEqualTo(2);
        assertThat(mapped.getNounDensity()).as("chunk noun density").isEqualTo(0.30f);
        assertThat(mapped.getRelativePosition()).as("chunk relative position").isEqualTo(0.35f);
        assertThat(mapped.getIsFirstChunk()).as("not first chunk").isFalse();
        assertThat(mapped.getLexicalDensity()).as("chunk lexical density").isEqualTo(0.55f);

        var withoutAnalytics = osDoc.getSemanticSets(0).getEmbeddingsList().stream()
                .filter(e -> !e.hasChunkAnalytics())
                .findFirst();
        assertThat(withoutAnalytics).as("embedding without chunk_analytics present").isPresent();
    }

    @Test
    void fullAnalyticsPipeline_allThreeLevelsPopulated() {
        // End-to-end: a realistic PipeDoc with all three levels of analytics
        SourceFieldAnalytics sfa = SourceFieldAnalytics.newBuilder()
                .setSourceField("body")
                .setChunkConfigId("token_500")
                .setDocumentAnalytics(DocumentAnalytics.newBuilder()
                        .setWordCount(300).setSentenceCount(15).setDetectedLanguage("eng").build())
                .setTotalChunks(2)
                .build();

        NlpDocumentAnalysis nlp = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage("eng")
                .setNounDensity(0.25f)
                .addSentences(SentenceSpan.newBuilder().setText("Hello world.").build())
                .build();

        ChunkAnalytics ca1 = ChunkAnalytics.newBuilder()
                .setWordCount(160).setRelativePosition(0.0f).setIsFirstChunk(true).build();
        ChunkAnalytics ca2 = ChunkAnalytics.newBuilder()
                .setWordCount(140).setRelativePosition(0.5f).setIsLastChunk(true).build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("full-analytics")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSourceFieldAnalytics(sfa)
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("token_500")
                                .setEmbeddingConfigId("all-MiniLM")
                                .setNlpAnalysis(nlp)
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkAnalytics(ca1)
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("first chunk").addVector(0.1f).build()))
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkAnalytics(ca2)
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("second chunk").addVector(0.2f).build()))
                                .build())
                        .build())
                .build();

        ConversionResult result = converter.convertWithAuditLog(doc);
        OpenSearchDocument osDoc = result.document();

        // Document-level
        assertThat(osDoc.getSourceFieldAnalyticsCount()).as("source field analytics").isEqualTo(1);
        assertThat(osDoc.getSourceFieldAnalytics(0).getDocumentAnalytics().getWordCount())
                .as("document word count").isEqualTo(300);

        // NLP-level
        assertThat(osDoc.hasNlpAnalysis()).as("NLP analysis present").isTrue();
        assertThat(osDoc.getNlpAnalysis().getDetectedLanguage()).as("NLP language").isEqualTo("eng");

        // Chunk-level
        assertThat(osDoc.getSemanticSets(0).getEmbeddingsCount()).as("2 embeddings").isEqualTo(2);
        long chunksWithAnalytics = osDoc.getSemanticSets(0).getEmbeddingsList().stream()
                .filter(e -> e.hasChunkAnalytics())
                .count();
        assertThat(chunksWithAnalytics).as("both chunks have analytics").isEqualTo(2);

        // Audit trail
        assertThat(result.auditLogs())
                .as("audit log covers all analytics levels")
                .anyMatch(l -> l.contains("source field analytics"))
                .anyMatch(l -> l.contains("NLP analysis"));
    }

    @Test
    void analyticsMapping_jsonRoundTrip_fieldsPreserved() throws Exception {
        // Verify the OpenSearchDocument serializes to JSON with analytics intact
        // — this is what actually gets sent to OpenSearch
        ChunkAnalytics ca = ChunkAnalytics.newBuilder()
                .setWordCount(50).setNounDensity(0.30f).setRelativePosition(0.5f)
                .setIsFirstChunk(false).setIsLastChunk(true)
                .build();

        NlpDocumentAnalysis nlp = NlpDocumentAnalysis.newBuilder()
                .setDetectedLanguage("eng").setNounDensity(0.25f).setLexicalDensity(0.52f)
                .build();

        SourceFieldAnalytics sfa = SourceFieldAnalytics.newBuilder()
                .setSourceField("body").setChunkConfigId("c1")
                .setDocumentAnalytics(DocumentAnalytics.newBuilder().setWordCount(200).build())
                .build();

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("json-test")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSourceFieldAnalytics(sfa)
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setSourceFieldName("body").setChunkConfigId("c1").setEmbeddingConfigId("m1")
                                .setNlpAnalysis(nlp)
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkAnalytics(ca)
                                        .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                                                .setTextContent("text").addVector(0.1f).build()))
                                .build())
                        .build())
                .build();

        OpenSearchDocument osDoc = converter.convertToOpenSearchDocument(doc);

        // Serialize to JSON (what gets sent to OpenSearch)
        String json = com.google.protobuf.util.JsonFormat.printer().print(osDoc);

        // Deserialize back and verify round-trip
        OpenSearchDocument.Builder roundTripBuilder = OpenSearchDocument.newBuilder();
        com.google.protobuf.util.JsonFormat.parser().ignoringUnknownFields().merge(json, roundTripBuilder);
        OpenSearchDocument roundTrip = roundTripBuilder.build();

        assertThat(roundTrip.getSourceFieldAnalyticsCount())
                .as("source field analytics survives JSON round-trip").isEqualTo(1);
        assertThat(roundTrip.getSourceFieldAnalytics(0).getDocumentAnalytics().getWordCount())
                .as("word count survives round-trip").isEqualTo(200);
        assertThat(roundTrip.getNlpAnalysis().getDetectedLanguage())
                .as("NLP language survives round-trip").isEqualTo("eng");
        assertThat(roundTrip.getSemanticSets(0).getEmbeddings(0).hasChunkAnalytics())
                .as("chunk analytics survives round-trip").isTrue();
        assertThat(roundTrip.getSemanticSets(0).getEmbeddings(0).getChunkAnalytics().getWordCount())
                .as("chunk word count survives round-trip").isEqualTo(50);
        assertThat(roundTrip.getSemanticSets(0).getEmbeddings(0).getChunkAnalytics().getRelativePosition())
                .as("chunk relative position survives round-trip").isEqualTo(0.5f);
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
