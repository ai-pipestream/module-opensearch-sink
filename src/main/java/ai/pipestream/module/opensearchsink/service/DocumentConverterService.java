package ai.pipestream.module.opensearchsink.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.opensearch.v1.OpenSearchEmbedding;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.OperationContainer;

import java.util.*;

@ApplicationScoped
public class DocumentConverterService {

    private static final Logger LOG = Logger.getLogger(DocumentConverterService.class);

    /**
     * Prepare a gRPC BulkRequest for indexing the document via DocumentService.
     */
    public BulkRequest prepareBulkRequest(PipeDoc document, String indexName) {
        OpenSearchDocument osDoc = convertToOpenSearchDocument(document);

        try {
            String jsonDoc = JsonFormat.printer().print(osDoc);
            ByteString docBytes = ByteString.copyFromUtf8(jsonDoc);

            IndexOperation indexOp = IndexOperation.newBuilder()
                    .setXIndex(indexName)
                    .setXId(document.getDocId())
                    .build();

            BulkRequestBody body = BulkRequestBody.newBuilder()
                    .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOp).build())
                    .setObject(docBytes)
                    .build();

            return BulkRequest.newBuilder()
                    .setIndex(indexName)
                    .addBulkRequestBody(body)
                    .build();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to convert document %s to JSON", document.getDocId());
            throw new RuntimeException("Document conversion failed", e);
        }
    }

    public OpenSearchDocument convertToOpenSearchDocument(PipeDoc document) {
        // Use last_modified_date for created_at if available, otherwise use current time
        Timestamp createdAt = document.getSearchMetadata().hasCreationDate() 
            ? document.getSearchMetadata().getCreationDate()
            : (document.getSearchMetadata().hasLastModifiedDate() 
                ? document.getSearchMetadata().getLastModifiedDate()
                : Timestamp.getDefaultInstance());
        
        OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder()
                .setOriginalDocId(document.getDocId())
                .setDocType(document.getSearchMetadata().getDocumentType())
                .setCreatedBy("") // TODO: Populate from PipeDoc metadata when available
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

        // Convert all embeddings to nested structure
        populateSemanticSets(document, builder);

        // Handle custom fields if present
        if (document.getSearchMetadata().hasCustomFields()) {
            builder.setCustomFields(document.getSearchMetadata().getCustomFields());
        }

        return builder.build();
    }

    private void populateSemanticSets(PipeDoc document, OpenSearchDocument.Builder builder) {
        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            String chunkConfigId = result.getChunkConfigId();
            String embeddingId = result.getEmbeddingConfigId();
            String sourceFieldName = result.getSourceFieldName();

            if (chunkConfigId == null || chunkConfigId.isEmpty() || 
                embeddingId == null || embeddingId.isEmpty()) {
                continue;
            }

            SemanticVectorSet.Builder setBuilder = SemanticVectorSet.newBuilder()
                    .setSourceFieldName(sourceFieldName != null ? sourceFieldName : "unknown")
                    .setChunkConfigId(chunkConfigId)
                    .setEmbeddingId(embeddingId);

            // Deduplicate embeddings within this set by source_text
            Map<Integer, OpenSearchEmbedding> embeddingMap = new HashMap<>();

            for (SemanticChunk chunk : result.getChunksList()) {
                if (!chunk.hasEmbeddingInfo() || chunk.getEmbeddingInfo().getVectorCount() == 0) {
                    continue;
                }

                String sourceText = chunk.getEmbeddingInfo().getTextContent();
                int textHash = sourceText.hashCode();
                
                if (!embeddingMap.containsKey(textHash)) {
                    OpenSearchEmbedding.Builder embeddingBuilder = OpenSearchEmbedding.newBuilder()
                            .addAllVector(chunk.getEmbeddingInfo().getVectorList())
                            .setSourceText(sourceText)
                            .setChunkConfigId(chunkConfigId)
                            .setEmbeddingId(embeddingId)
                            .setIsPrimary(isPrimaryEmbedding(chunk, result));

                    embeddingMap.put(textHash, embeddingBuilder.build());
                }
            }

            if (!embeddingMap.isEmpty()) {
                setBuilder.addAllEmbeddings(embeddingMap.values());
                builder.addSemanticSets(setBuilder.build());
            }
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
}
