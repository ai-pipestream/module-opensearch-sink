package io.pipeline.module.opensearchsink.service;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.SemanticChunk;
import io.pipeline.data.v1.SemanticProcessingResult;
import io.pipeline.opensearch.v1.Embedding;
import io.pipeline.opensearch.v1.OpenSearchDocument;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;

import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class DocumentConverterService {

    private static final Logger LOG = Logger.getLogger(DocumentConverterService.class);

    public List<BulkOperation> prepareBulkOperations(PipeDoc document, String indexName) {
        OpenSearchDocument osDoc = convertToOpenSearchDocument(document);
        
        try {
            String jsonDoc = JsonFormat.printer().print(osDoc);
            
            IndexOperation<String> indexOp = new IndexOperation.Builder<String>()
                    .index(indexName)
                    .id(document.getDocId())
                    .document(jsonDoc)
                    .versionType(org.opensearch.client.opensearch._types.VersionType.External)
                    .version((long) document.getSearchMetadata().getLastModifiedDate().getSeconds())
                    .build();

            return List.of(new BulkOperation.Builder().index(indexOp).build());
        } catch (Exception e) {
            LOG.errorf(e, "Failed to convert document %s to JSON", document.getDocId());
            throw new RuntimeException("Document conversion failed", e);
        }
    }

    private OpenSearchDocument convertToOpenSearchDocument(PipeDoc document) {
        OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder()
                .setOriginalDocId(document.getDocId())
                .setDocType(document.getSearchMetadata().getDocumentType())
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
        // Revision ID not available in current PipeDoc structure
        // if (document.hasRevisionId()) {
        //     builder.setRevisionId(document.getRevisionId());
        // }

        // Convert all embeddings to nested structure
        List<Embedding> embeddings = extractAllEmbeddings(document);
        builder.addAllEmbeddings(embeddings);

        // Handle custom fields if present
        if (document.getSearchMetadata().hasCustomFields()) {
            builder.setCustomFields(document.getSearchMetadata().getCustomFields());
        }

        return builder.build();
    }

    private List<Embedding> extractAllEmbeddings(PipeDoc document) {
        List<Embedding> embeddings = new ArrayList<>();
        
        // Deduplicate embeddings by composite key (chunk_config_id + embedding_id + source_text)
        Map<String, Embedding> embeddingMap = new HashMap<>();

        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            String chunkConfigId = result.getChunkConfigId();
            String embeddingId = result.getEmbeddingConfigId();
            
            for (SemanticChunk chunk : result.getChunksList()) {
                if (!chunk.hasEmbeddingInfo() || chunk.getEmbeddingInfo().getVectorCount() == 0) {
                    continue;
                }

                String sourceText = chunk.getEmbeddingInfo().getTextContent();
                String compositeKey = chunkConfigId + "|" + embeddingId + "|" + sourceText.hashCode();
                
                if (!embeddingMap.containsKey(compositeKey)) {
                    Embedding.Builder embeddingBuilder = Embedding.newBuilder()
                            .addAllVector(chunk.getEmbeddingInfo().getVectorList())
                            .setSourceText(sourceText)
                            .setChunkConfigId(chunkConfigId)
                            .setEmbeddingId(embeddingId)
                            .setIsPrimary(isPrimaryEmbedding(chunk, result));

                    // Add context text if available
                    // Context text not available in current ChunkEmbedding structure
                    // if (chunk.getEmbeddingInfo().getContextTextCount() > 0) {
                    //     embeddingBuilder.addAllContextText(chunk.getEmbeddingInfo().getContextTextList());
                    // }

                    embeddingMap.put(compositeKey, embeddingBuilder.build());
                }
            }
        }

        return new ArrayList<>(embeddingMap.values());
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
}