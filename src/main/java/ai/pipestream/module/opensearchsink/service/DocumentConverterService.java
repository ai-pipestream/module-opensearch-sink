package ai.pipestream.module.opensearchsink.service;

import com.google.protobuf.Timestamp;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.opensearch.v1.OpenSearchEmbedding;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;

@ApplicationScoped
public class DocumentConverterService {

    private static final Logger LOG = Logger.getLogger(DocumentConverterService.class);

    public OpenSearchDocument convertToOpenSearchDocument(PipeDoc document) {
        return convertWithAuditLog(document).document();
    }

    public ConversionResult convertWithAuditLog(PipeDoc document) {
        List<String> auditLogs = new ArrayList<>();

        // Use last_modified_date for created_at if available, otherwise use current time
        Timestamp createdAt = document.getSearchMetadata().hasCreationDate()
            ? document.getSearchMetadata().getCreationDate()
            : (document.getSearchMetadata().hasLastModifiedDate()
                ? document.getSearchMetadata().getLastModifiedDate()
                : Timestamp.getDefaultInstance());

        OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder()
                .setOriginalDocId(document.getDocId())
                .setDocType(document.getSearchMetadata().getDocumentType())
                .setCreatedBy("")
                .setCreatedAt(createdAt)
                .setLastModifiedAt(document.getSearchMetadata().getLastModifiedDate());

        // Set optional fields from PipeDoc search metadata
        if (document.getSearchMetadata().hasSourceUri()) {
            builder.setSourceUri(document.getSearchMetadata().getSourceUri());
        }
        if (document.getSearchMetadata().hasSourceMimeType()) {
            builder.setSourceMimeType(document.getSearchMetadata().getSourceMimeType());
        }
        if (document.getSearchMetadata().hasTitle()) {
            builder.setTitle(document.getSearchMetadata().getTitle());
        }
        if (document.getSearchMetadata().hasBody()) {
            builder.setBody(document.getSearchMetadata().getBody());
        }
        if (document.getSearchMetadata().hasKeywords() && document.getSearchMetadata().getKeywords().getKeywordCount() > 0) {
            builder.addAllTags(document.getSearchMetadata().getKeywords().getKeywordList());
        }

        int semanticResultCount = document.getSearchMetadata().getSemanticResultsCount();
        auditLogs.add("Document has " + semanticResultCount + " semantic result sets");

        // Populate document-level analytics from PipeDoc
        if (document.getSearchMetadata().getSourceFieldAnalyticsCount() > 0) {
            builder.addAllSourceFieldAnalytics(document.getSearchMetadata().getSourceFieldAnalyticsList());
            auditLogs.add("Mapped " + document.getSearchMetadata().getSourceFieldAnalyticsCount()
                    + " source field analytics entries");
        }

        // Populate NLP analysis from first SemanticProcessingResult that has it
        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            if (result.hasNlpAnalysis()) {
                builder.setNlpAnalysis(result.getNlpAnalysis());
                auditLogs.add("Mapped NLP analysis (language=" + result.getNlpAnalysis().getDetectedLanguage() + ")");
                break;  // One NLP pass shared across all results
            }
        }

        // Convert all embeddings to nested structure
        populateSemanticSets(document, builder, auditLogs);

        // Handle custom fields and metadata map
        com.google.protobuf.Struct.Builder customFieldsBuilder = com.google.protobuf.Struct.newBuilder();
        boolean hasCustomFields = false;

        // Copy existing custom_fields if present
        if (document.getSearchMetadata().hasCustomFields()) {
            customFieldsBuilder.putAllFields(document.getSearchMetadata().getCustomFields().getFieldsMap());
            hasCustomFields = true;
        }

        // Promote metadata map entries into custom_fields so they're searchable in OpenSearch
        if (document.getSearchMetadata().getMetadataCount() > 0) {
            for (var entry : document.getSearchMetadata().getMetadataMap().entrySet()) {
                customFieldsBuilder.putFields(entry.getKey(),
                        com.google.protobuf.Value.newBuilder().setStringValue(entry.getValue()).build());
            }
            hasCustomFields = true;
            auditLogs.add("Promoted " + document.getSearchMetadata().getMetadataCount() + " metadata entries to custom_fields");
        }

        if (hasCustomFields) {
            builder.setCustomFields(customFieldsBuilder.build());
        }

        return new ConversionResult(builder.build(), auditLogs);
    }

    private void populateSemanticSets(PipeDoc document, OpenSearchDocument.Builder builder, List<String> auditLogs) {
        int totalEmbeddings = 0;
        int totalChunksProcessed = 0;
        int totalChunksDropped = 0;
        int totalDeduplicated = 0;
        int setsProcessed = 0;

        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            String chunkConfigId = result.getChunkConfigId();
            String embeddingId = result.getEmbeddingConfigId();
            String sourceFieldName = result.getSourceFieldName();
            String resultSetName = result.getResultSetName();

            if (chunkConfigId == null || chunkConfigId.isEmpty() ||
                embeddingId == null || embeddingId.isEmpty()) {
                String setDesc = resultSetName != null && !resultSetName.isEmpty() ? resultSetName : "(unnamed)";
                auditLogs.add("Warning: Skipping semantic result set '" + setDesc
                        + "' — missing chunk_config_id or embedding_config_id");
                LOG.warnf("Skipping semantic result set '%s' for doc %s: chunk_config_id='%s', embedding_config_id='%s'",
                        setDesc, document.getDocId(), chunkConfigId, embeddingId);
                continue;
            }

            SemanticVectorSet.Builder setBuilder = SemanticVectorSet.newBuilder()
                    .setSourceFieldName(sourceFieldName != null ? sourceFieldName : "unknown")
                    .setChunkConfigId(chunkConfigId)
                    .setEmbeddingId(embeddingId);
            // Propagate semantic config reference for semantic resolution path
            if (result.hasSemanticConfigId() && !result.getSemanticConfigId().isEmpty()) {
                setBuilder.setSemanticConfigId(result.getSemanticConfigId());
            }
            if (result.hasSemanticGranularity() && !result.getSemanticGranularity().isEmpty()) {
                setBuilder.setGranularity(mapStringToGranularity(result.getSemanticGranularity()));
            }

            // Deduplicate embeddings within this set by source_text
            Map<String, OpenSearchEmbedding> embeddingMap = new LinkedHashMap<>();
            int chunksInSet = result.getChunksCount();
            int droppedInSet = 0;
            int dedupedInSet = 0;

            for (SemanticChunk chunk : result.getChunksList()) {
                if (!chunk.hasEmbeddingInfo() || chunk.getEmbeddingInfo().getVectorCount() == 0) {
                    droppedInSet++;
                    continue;
                }

                String sourceText = chunk.getEmbeddingInfo().getTextContent();

                if (!embeddingMap.containsKey(sourceText)) {
                    OpenSearchEmbedding.Builder embeddingBuilder = OpenSearchEmbedding.newBuilder()
                            .addAllVector(chunk.getEmbeddingInfo().getVectorList())
                            .setSourceText(sourceText)
                            .setChunkConfigId(chunkConfigId)
                            .setEmbeddingId(embeddingId)
                            .setIsPrimary(isPrimaryEmbedding(chunk, result));

                    // Transfer per-chunk analytics if available
                    if (chunk.hasChunkAnalytics()) {
                        embeddingBuilder.setChunkAnalytics(chunk.getChunkAnalytics());
                    }

                    embeddingMap.put(sourceText, embeddingBuilder.build());
                } else {
                    dedupedInSet++;
                }
            }

            if (droppedInSet > 0) {
                auditLogs.add("Warning: " + droppedInSet + " of " + chunksInSet
                        + " chunks had no vectors and were excluded from vector set '"
                        + chunkConfigId + "/" + embeddingId + "'");
                LOG.warnf("Dropped %d chunks without vectors from set %s/%s for doc %s",
                        droppedInSet, chunkConfigId, embeddingId, document.getDocId());
            }

            if (dedupedInSet > 0) {
                auditLogs.add("Deduplication removed " + dedupedInSet
                        + " duplicate embeddings from vector set '" + chunkConfigId + "/" + embeddingId + "'");
            }

            if (!embeddingMap.isEmpty()) {
                setBuilder.addAllEmbeddings(embeddingMap.values());
                builder.addSemanticSets(setBuilder.build());
            }

            totalChunksProcessed += chunksInSet;
            totalChunksDropped += droppedInSet;
            totalDeduplicated += dedupedInSet;
            totalEmbeddings += embeddingMap.size();
            setsProcessed++;
        }

        auditLogs.add("Extracted " + totalEmbeddings + " embeddings from " + totalChunksProcessed
                + " chunks across " + setsProcessed + " vector sets");
        if (totalChunksDropped > 0) {
            auditLogs.add("Warning: " + totalChunksDropped + " total chunks had no vectors and were excluded from indexing");
        }
        if (totalDeduplicated > 0) {
            auditLogs.add("Deduplication removed " + totalDeduplicated + " total duplicate embeddings");
        }
    }

    private boolean isPrimaryEmbedding(SemanticChunk chunk, SemanticProcessingResult result) {
        // Primary embeddings are typically from non-chunked fields like title, author, etc.
        // This is a heuristic - you may want to make this configurable
        String chunkConfigId = result.getChunkConfigId();
        return chunkConfigId != null &&
               (chunkConfigId.contains("title") ||
                chunkConfigId.contains("author") ||
                chunkConfigId.contains("summary") ||
                !chunkConfigId.contains("chunk"));
    }

    private ai.pipestream.opensearch.v1.SemanticGranularity mapStringToGranularity(String granularity) {
        return switch (granularity) {
            case "SEMANTIC_CHUNK" -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_SEMANTIC_CHUNK;
            case "SENTENCE" -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_SENTENCE;
            case "PARAGRAPH" -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_PARAGRAPH;
            case "SECTION" -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_SECTION;
            case "DOCUMENT" -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_DOCUMENT;
            default -> ai.pipestream.opensearch.v1.SemanticGranularity.SEMANTIC_GRANULARITY_UNSPECIFIED;
        };
    }
}
