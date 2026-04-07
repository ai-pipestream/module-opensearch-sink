package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.opensearch.v1.*;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;

/**
 * Converts a PipeDoc + OpenSearchDocument into flat chunk documents for the
 * CHUNK_COMBINED and SEPARATE_INDICES indexing strategies.
 *
 * <p>Produces an {@link OpenSearchDocumentMap} (parent doc without vectors) and a list of
 * {@link OpenSearchChunkDocument} rows (one per chunk or one per embedding, depending on strategy).
 */
@ApplicationScoped
public class ChunkDocumentConverter {

    private static final Logger LOG = Logger.getLogger(ChunkDocumentConverter.class);

    /**
     * Convert a PipeDoc + OpenSearchDocument into a parent document map and flat chunk documents.
     *
     * @param doc            the source PipeDoc (used for ownership/ACL)
     * @param osDoc          the already-converted OpenSearchDocument with semantic sets
     * @param baseIndexName  the base index name to derive chunk index names from
     * @param strategy       CHUNK_COMBINED or SEPARATE_INDICES
     * @return conversion result with document map, chunk documents, and audit logs
     */
    public ChunkConversionResult convertToChunks(
            PipeDoc doc,
            OpenSearchDocument osDoc,
            String baseIndexName,
            IndexingStrategy strategy) {

        List<String> auditLogs = new ArrayList<>();

        // Build ACL struct from PipeDoc ownership
        Struct acl = buildAcl(doc, auditLogs);

        // Build the parent document map (no vectors)
        OpenSearchDocumentMap documentMap = buildDocumentMap(osDoc, acl, baseIndexName, strategy, auditLogs);

        // Build flat chunk documents
        List<OpenSearchChunkDocument> chunkDocuments;
        if (strategy == IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED) {
            chunkDocuments = buildChunkCombinedDocuments(osDoc, acl, auditLogs);
        } else {
            chunkDocuments = buildSeparateIndicesDocuments(osDoc, acl, auditLogs);
        }

        auditLogs.add("Converted " + chunkDocuments.size() + " chunks across "
                + countDistinctChunkConfigs(osDoc) + " chunk configs for doc " + osDoc.getOriginalDocId());

        return new ChunkConversionResult(documentMap, chunkDocuments, auditLogs);
    }

    /**
     * Sanitize an identifier for use in index names: replace non-alphanumeric characters with underscores.
     */
    static String sanitize(String id) {
        if (id == null || id.isEmpty()) {
            return "unknown";
        }
        return id.replaceAll("[^a-zA-Z0-9]", "_");
    }

    /**
     * Build the chunk index name based on strategy.
     */
    static String buildChunkIndexName(String baseIndexName, String chunkConfigId, String embeddingId, IndexingStrategy strategy) {
        if (strategy == IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED) {
            return baseIndexName + "--chunk--" + sanitize(chunkConfigId);
        } else {
            return baseIndexName + "--vs--" + sanitize(chunkConfigId) + "--" + sanitize(embeddingId);
        }
    }

    // ---- private helpers ----

    private Struct buildAcl(PipeDoc doc, List<String> auditLogs) {
        Struct.Builder aclBuilder = Struct.newBuilder();
        if (doc.hasOwnership()) {
            aclBuilder.putFields("account_id",
                    Value.newBuilder().setStringValue(doc.getOwnership().getAccountId()).build());
            aclBuilder.putFields("datasource_id",
                    Value.newBuilder().setStringValue(doc.getOwnership().getDatasourceId()).build());
            if (doc.getOwnership().hasConnectorId()) {
                aclBuilder.putFields("connector_id",
                        Value.newBuilder().setStringValue(doc.getOwnership().getConnectorId()).build());
            }
            auditLogs.add("Built ACL from ownership context: account_id="
                    + doc.getOwnership().getAccountId() + ", datasource_id=" + doc.getOwnership().getDatasourceId());
        } else {
            auditLogs.add("No ownership context on PipeDoc — ACL will be empty");
        }
        return aclBuilder.build();
    }

    private OpenSearchDocumentMap buildDocumentMap(
            OpenSearchDocument osDoc,
            Struct acl,
            String baseIndexName,
            IndexingStrategy strategy,
            List<String> auditLogs) {

        OpenSearchDocumentMap.Builder mapBuilder = OpenSearchDocumentMap.newBuilder()
                .setOriginalDocId(osDoc.getOriginalDocId())
                .setDocType(osDoc.getDocType())
                .setCreatedBy(osDoc.getCreatedBy());

        if (osDoc.hasCreatedAt()) {
            mapBuilder.setCreatedAt(osDoc.getCreatedAt());
        }
        if (osDoc.hasLastModifiedAt()) {
            mapBuilder.setLastModifiedAt(osDoc.getLastModifiedAt());
        }
        if (osDoc.hasSourceUri()) {
            mapBuilder.setSourceUri(osDoc.getSourceUri());
        }
        if (osDoc.hasSourceMimeType()) {
            mapBuilder.setSourceMimeType(osDoc.getSourceMimeType());
        }
        if (osDoc.hasTitle()) {
            mapBuilder.setTitle(osDoc.getTitle());
        }
        if (osDoc.hasBody()) {
            mapBuilder.setBody(osDoc.getBody());
        }
        if (osDoc.getTagsCount() > 0) {
            mapBuilder.addAllTags(osDoc.getTagsList());
        }
        if (osDoc.hasRevisionId()) {
            mapBuilder.setRevisionId(osDoc.getRevisionId());
        }
        if (osDoc.hasCustomFields()) {
            mapBuilder.setCustomFields(osDoc.getCustomFields());
        }
        if (osDoc.getSourceFieldAnalyticsCount() > 0) {
            mapBuilder.addAllSourceFieldAnalytics(osDoc.getSourceFieldAnalyticsList());
        }
        if (osDoc.hasNlpAnalysis()) {
            mapBuilder.setNlpAnalysis(osDoc.getNlpAnalysis());
        }

        // Set ACL
        mapBuilder.setAcl(acl);

        // Build chunk index references by grouping semantic sets
        Map<String, ChunkIndexReferenceAccumulator> refMap = new LinkedHashMap<>();
        for (SemanticVectorSet svs : osDoc.getSemanticSetsList()) {
            String chunkConfigId = svs.getChunkConfigId();
            String embeddingId = svs.getEmbeddingId();

            if (strategy == IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED) {
                String indexName = buildChunkIndexName(baseIndexName, chunkConfigId, null, strategy);
                refMap.computeIfAbsent(chunkConfigId, k -> new ChunkIndexReferenceAccumulator(indexName, chunkConfigId))
                        .addEmbeddingModel(embeddingId)
                        .updateChunkCount(svs.getEmbeddingsCount());
            } else {
                String key = chunkConfigId + "|" + embeddingId;
                String indexName = buildChunkIndexName(baseIndexName, chunkConfigId, embeddingId, strategy);
                refMap.computeIfAbsent(key, k -> new ChunkIndexReferenceAccumulator(indexName, chunkConfigId))
                        .addEmbeddingModel(embeddingId)
                        .updateChunkCount(svs.getEmbeddingsCount());
            }
        }

        for (ChunkIndexReferenceAccumulator acc : refMap.values()) {
            mapBuilder.addChunkIndices(acc.build());
        }

        auditLogs.add("Built document map with " + refMap.size() + " chunk index references for doc "
                + osDoc.getOriginalDocId());

        return mapBuilder.build();
    }

    /**
     * CHUNK_COMBINED: group embeddings by (chunk_config_id, source_text).
     * Each unique (chunk_config_id, source_text) produces one OpenSearchChunkDocument
     * with multiple entries in the embeddings map (one per embedding model).
     */
    private List<OpenSearchChunkDocument> buildChunkCombinedDocuments(
            OpenSearchDocument osDoc,
            Struct acl,
            List<String> auditLogs) {

        // Group by chunk_config_id, then by source_text
        // Key: chunk_config_id -> ordered list of unique source texts with their accumulated data
        Map<String, LinkedHashMap<String, ChunkAccumulator>> configGroups = new LinkedHashMap<>();

        for (SemanticVectorSet svs : osDoc.getSemanticSetsList()) {
            String chunkConfigId = svs.getChunkConfigId();
            String embeddingId = svs.getEmbeddingId();
            String sourceField = svs.getSourceFieldName();

            LinkedHashMap<String, ChunkAccumulator> textGroup =
                    configGroups.computeIfAbsent(chunkConfigId, k -> new LinkedHashMap<>());

            for (OpenSearchEmbedding emb : svs.getEmbeddingsList()) {
                String sourceText = emb.getSourceText();
                ChunkAccumulator acc = textGroup.computeIfAbsent(sourceText,
                        k -> new ChunkAccumulator(sourceField, chunkConfigId, sourceText, emb.getIsPrimary()));

                // Add this embedding model's vector
                acc.addEmbedding(embeddingId, emb.getVectorList());

                // Preserve chunk analytics from whichever embedding has it
                if (emb.hasChunkAnalytics() && acc.chunkAnalytics == null) {
                    acc.chunkAnalytics = emb.getChunkAnalytics();
                }
            }
        }

        // Now flatten into OpenSearchChunkDocuments
        List<OpenSearchChunkDocument> result = new ArrayList<>();
        for (var configEntry : configGroups.entrySet()) {
            int chunkIndex = 0;
            for (var textEntry : configEntry.getValue().entrySet()) {
                ChunkAccumulator acc = textEntry.getValue();
                OpenSearchChunkDocument.Builder chunkBuilder = OpenSearchChunkDocument.newBuilder()
                        .setDocId(osDoc.getOriginalDocId())
                        .setDocType(osDoc.getDocType())
                        .setAcl(acl)
                        .setSourceField(acc.sourceField)
                        .setChunkConfigId(acc.chunkConfigId)
                        .setChunkIndex(chunkIndex)
                        .setSourceText(acc.sourceText)
                        .setIsPrimary(acc.isPrimary);

                if (osDoc.hasTitle()) {
                    chunkBuilder.setTitle(osDoc.getTitle());
                }
                if (osDoc.hasSourceUri()) {
                    chunkBuilder.setSourceUri(osDoc.getSourceUri());
                }
                if (acc.chunkAnalytics != null) {
                    chunkBuilder.setChunkAnalytics(acc.chunkAnalytics);
                }

                // Add all embedding vectors
                for (var embEntry : acc.embeddings.entrySet()) {
                    chunkBuilder.putEmbeddings(embEntry.getKey(),
                            FloatVector.newBuilder().addAllValues(embEntry.getValue()).build());
                }

                result.add(chunkBuilder.build());
                chunkIndex++;
            }
        }

        auditLogs.add("CHUNK_COMBINED: grouped into " + result.size() + " chunk documents across "
                + configGroups.size() + " chunk configs");
        return result;
    }

    /**
     * SEPARATE_INDICES: each embedding becomes its own chunk document with a single entry
     * in the embeddings map.
     */
    private List<OpenSearchChunkDocument> buildSeparateIndicesDocuments(
            OpenSearchDocument osDoc,
            Struct acl,
            List<String> auditLogs) {

        List<OpenSearchChunkDocument> result = new ArrayList<>();

        // Track chunk index per (chunk_config_id, embedding_id) pair
        Map<String, Integer> chunkIndexCounters = new HashMap<>();

        for (SemanticVectorSet svs : osDoc.getSemanticSetsList()) {
            String chunkConfigId = svs.getChunkConfigId();
            String embeddingId = svs.getEmbeddingId();
            String sourceField = svs.getSourceFieldName();
            String counterKey = chunkConfigId + "|" + embeddingId;

            for (OpenSearchEmbedding emb : svs.getEmbeddingsList()) {
                int chunkIndex = chunkIndexCounters.getOrDefault(counterKey, 0);
                chunkIndexCounters.put(counterKey, chunkIndex + 1);

                OpenSearchChunkDocument.Builder chunkBuilder = OpenSearchChunkDocument.newBuilder()
                        .setDocId(osDoc.getOriginalDocId())
                        .setDocType(osDoc.getDocType())
                        .setAcl(acl)
                        .setSourceField(sourceField)
                        .setChunkConfigId(chunkConfigId)
                        .setChunkIndex(chunkIndex)
                        .setSourceText(emb.getSourceText())
                        .setIsPrimary(emb.getIsPrimary());

                if (osDoc.hasTitle()) {
                    chunkBuilder.setTitle(osDoc.getTitle());
                }
                if (osDoc.hasSourceUri()) {
                    chunkBuilder.setSourceUri(osDoc.getSourceUri());
                }
                if (emb.hasChunkAnalytics()) {
                    chunkBuilder.setChunkAnalytics(emb.getChunkAnalytics());
                }

                chunkBuilder.putEmbeddings(embeddingId,
                        FloatVector.newBuilder().addAllValues(emb.getVectorList()).build());

                result.add(chunkBuilder.build());
            }
        }

        auditLogs.add("SEPARATE_INDICES: produced " + result.size() + " chunk documents");
        return result;
    }

    private int countDistinctChunkConfigs(OpenSearchDocument osDoc) {
        Set<String> configs = new HashSet<>();
        for (SemanticVectorSet svs : osDoc.getSemanticSetsList()) {
            configs.add(svs.getChunkConfigId());
        }
        return configs.size();
    }

    // ---- inner helper classes ----

    /**
     * Accumulates data for building a ChunkIndexReference.
     */
    private static class ChunkIndexReferenceAccumulator {
        private final String indexName;
        private final String chunkConfigId;
        private final Set<String> embeddingModels = new LinkedHashSet<>();
        private int chunkCount = 0;

        ChunkIndexReferenceAccumulator(String indexName, String chunkConfigId) {
            this.indexName = indexName;
            this.chunkConfigId = chunkConfigId;
        }

        ChunkIndexReferenceAccumulator addEmbeddingModel(String model) {
            embeddingModels.add(model);
            return this;
        }

        ChunkIndexReferenceAccumulator updateChunkCount(int count) {
            // For CHUNK_COMBINED, multiple sets may contribute to the same config;
            // use max since they share the same text chunks
            this.chunkCount = Math.max(this.chunkCount, count);
            return this;
        }

        ChunkIndexReference build() {
            return ChunkIndexReference.newBuilder()
                    .setIndexName(indexName)
                    .setChunkConfigId(chunkConfigId)
                    .setChunkCount(chunkCount)
                    .addAllEmbeddingModels(embeddingModels)
                    .build();
        }
    }

    /**
     * Accumulates chunk data across multiple embedding models for CHUNK_COMBINED grouping.
     */
    private static class ChunkAccumulator {
        final String sourceField;
        final String chunkConfigId;
        final String sourceText;
        final boolean isPrimary;
        final Map<String, List<Float>> embeddings = new LinkedHashMap<>();
        ai.pipestream.data.v1.ChunkAnalytics chunkAnalytics;

        ChunkAccumulator(String sourceField, String chunkConfigId, String sourceText, boolean isPrimary) {
            this.sourceField = sourceField;
            this.chunkConfigId = chunkConfigId;
            this.sourceText = sourceText;
            this.isPrimary = isPrimary;
        }

        void addEmbedding(String embeddingId, List<Float> vector) {
            embeddings.put(embeddingId, new ArrayList<>(vector));
        }
    }
}
