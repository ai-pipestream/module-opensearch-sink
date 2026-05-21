package ai.pipestream.module.opensearchsink.work;

import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.server.work.ModuleProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses {@link OpenSearchSinkOptions} from {@code StreamMetadata.context_params}
 * as flattened by the engine ({@code GraphNode.custom_config.json_config}).
 *
 * <p>{@code plan_ids} is carried as a JSON array string in {@code plan_ids}
 * (e.g. {@code ["plan-a","plan-b"]}) because the engine flatten step only
 * copies scalar top-level json_config fields into context_params today.
 */
final class OpenSearchSinkOptionsParser {

    private static final String KEY_OPENSEARCH_INSTANCE = "opensearch_instance";
    private static final String KEY_PLAN_IDS = "plan_ids";

    private OpenSearchSinkOptionsParser() {}

    static OpenSearchSinkOptions parse(Map<String, String> contextParams, ObjectMapper objectMapper, String nodeId) {
        if (contextParams == null || contextParams.isEmpty()) {
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink node '" + nodeId + "' requires graph config in context_params; none present");
        }

        String opensearchInstance = requiredString(contextParams, KEY_OPENSEARCH_INSTANCE, nodeId);
        List<String> planIds = parsePlanIds(contextParams.get(KEY_PLAN_IDS), objectMapper, nodeId);

        return new OpenSearchSinkOptions(opensearchInstance, planIds, null);
    }

    private static String requiredString(Map<String, String> params, String key, String nodeId) {
        String raw = params.get(key);
        if (raw == null || raw.isBlank()) {
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink node '" + nodeId + "': context_params['" + key + "'] is required");
        }
        return raw.trim();
    }

    private static List<String> parsePlanIds(String raw, ObjectMapper objectMapper, String nodeId) {
        if (raw == null || raw.isBlank()) {
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink node '" + nodeId + "': context_params['" + KEY_PLAN_IDS + "'] is required");
        }
        String trimmed = raw.trim();
        try {
            if (trimmed.startsWith("[")) {
                JsonNode node = objectMapper.readTree(trimmed);
                if (!node.isArray()) {
                    throw new ModuleProcessor.PermanentFailure(
                            "opensearch-sink node '" + nodeId + "': context_params['" + KEY_PLAN_IDS
                                    + "'] must be a JSON array");
                }
                List<String> ids = new ArrayList<>(node.size());
                for (JsonNode element : node) {
                    if (!element.isTextual()) {
                        throw new ModuleProcessor.PermanentFailure(
                                "opensearch-sink node '" + nodeId + "': plan_ids array entries must be strings");
                    }
                    ids.add(element.asText());
                }
                return List.copyOf(ids);
            }
            if (trimmed.contains(",")) {
                String[] parts = trimmed.split(",");
                List<String> ids = new ArrayList<>(parts.length);
                for (String part : parts) {
                    if (!part.isBlank()) {
                        ids.add(part.trim());
                    }
                }
                return List.copyOf(ids);
            }
            return List.of(trimmed);
        } catch (JsonProcessingException e) {
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink node '" + nodeId + "': context_params['" + KEY_PLAN_IDS
                            + "'] is not valid JSON: " + e.getOriginalMessage(), e);
        }
    }
}
