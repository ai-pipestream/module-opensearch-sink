package ai.pipestream.module.opensearchsink;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ai.pipestream.module.opensearchsink.config.OpenSearchSinkConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Service for managing OpenSearch settings with caching support.
 * This class provides methods to store and retrieve settings related to OpenSearch indices,
 * including dynamic field tracking, cluster settings, and pipeline settings.
 */
@ApplicationScoped
public class OpenSearchSettings {
    
    private static final Logger LOG = Logger.getLogger(OpenSearchSettings.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    // Cache for index schemas to avoid repeated Consul lookups
    private final Map<String, IndexSchema> indexSchemaCache = new ConcurrentHashMap<>();
    
    // Cache for dynamic fields to track new fields across requests
    private final Map<String, Map<String, String>> dynamicFieldsCache = new ConcurrentHashMap<>();
    
    @Inject
    OpenSearchSinkConfig sinkConfig;
    
    // ConsulClient injection removed for now - can be added later for schema storage
    
    /**
     * Get the index schema for the specified index name.
     * Returns from cache if present. When not cached, requires dimension (from opensearch-manager or document).
     *
     * @param indexName The name of the index
     * @param dimension Vector dimension when creating new schema; required for cache miss
     * @return The index schema, or null if not cached and dimension is invalid
     */
    public IndexSchema getIndexSchema(String indexName, int dimension) {
        LOG.debugf("Getting index schema for %s", indexName);

        if (indexSchemaCache.containsKey(indexName)) {
            LOG.debugf("Found index schema in local cache for %s", indexName);
            return indexSchemaCache.get(indexName);
        }

        if (dimension <= 0) {
            LOG.warnf("Cannot create index schema for %s: dimension must be > 0", indexName);
            return null;
        }

        LOG.debugf("Creating new index schema for %s (dimension=%d)", indexName, dimension);
        IndexSchema schema = new IndexSchema(indexName, dimension, "default", sinkConfig.vectorSpaceType());
        indexSchemaCache.put(indexName, schema);
        return schema;
    }
    
    /**
     * Store the index schema in Consul and update the local cache.
     * 
     * @param schema The index schema to store
     */
    public void storeIndexSchema(IndexSchema schema) {
        // Update local cache only for now
        indexSchemaCache.put(schema.getIndexName(), schema);
        LOG.debugf("Stored schema metadata locally: %s", schema.getIndexName());
        
        // TODO: Store in Consul when ConsulClient integration is enabled
    }
    
    /**
     * Track a new field for the specified index.
     * This method adds the field to the dynamic fields cache.
     * 
     * @param indexName The name of the index
     * @param fieldName The name of the field
     * @param fieldType The type of the field
     */
    public void trackField(String indexName, String fieldName, String fieldType) {
        Map<String, String> fields = dynamicFieldsCache.computeIfAbsent(indexName, k -> new ConcurrentHashMap<>());
        fields.put(fieldName, fieldType);
        LOG.debugf("Tracked new field %s of type %s for index %s", fieldName, fieldType, indexName);
    }
    
    /**
     * Get all tracked fields for the specified index.
     * 
     * @param indexName The name of the index
     * @return A map of field names to field types
     */
    public Map<String, String> getTrackedFields(String indexName) {
        return dynamicFieldsCache.getOrDefault(indexName, new HashMap<>());
    }
    
    /**
     * Get the cluster name.
     * 
     * @return The cluster name
     */
    public String getClusterName() {
        return sinkConfig.clusterName();
    }
    
    /**
     * Get the pipeline name.
     * 
     * @return The pipeline name
     */
    public String getPipelineName() {
        return sinkConfig.pipelineName();
    }
    
    /**
     * Get the default vector space type.
     * 
     * @return The default vector space type
     */
    public String getDefaultVectorSpaceType() {
        return sinkConfig.vectorSpaceType();
    }
    
    /**
     * Class representing an OpenSearch index schema.
     */
    public static class IndexSchema {
        private String indexName;
        private int vectorDimension;
        private String modelId;
        private String spaceType;
        private long createdTimestamp;
        
        // Default constructor for Jackson
        public IndexSchema() {
        }
        
        public IndexSchema(String indexName, int vectorDimension, String modelId, String spaceType) {
            this.indexName = indexName;
            this.vectorDimension = vectorDimension;
            this.modelId = modelId;
            this.spaceType = spaceType;
            this.createdTimestamp = System.currentTimeMillis();
        }
        
        public String getIndexName() {
            return indexName;
        }
        
        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }
        
        public int getVectorDimension() {
            return vectorDimension;
        }
        
        public void setVectorDimension(int vectorDimension) {
            this.vectorDimension = vectorDimension;
        }
        
        public String getModelId() {
            return modelId;
        }
        
        public void setModelId(String modelId) {
            this.modelId = modelId;
        }
        
        public String getSpaceType() {
            return spaceType;
        }
        
        public void setSpaceType(String spaceType) {
            this.spaceType = spaceType;
        }
        
        public long getCreatedTimestamp() {
            return createdTimestamp;
        }
        
        public void setCreatedTimestamp(long createdTimestamp) {
            this.createdTimestamp = createdTimestamp;
        }
    }
}