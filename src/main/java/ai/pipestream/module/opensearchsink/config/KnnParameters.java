package ai.pipestream.module.opensearchsink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.Optional;

/**
 * DRAFT - A reference point for our design conversation.
 * Defines the tuning parameters for the selected k-NN engine.
 */
@Schema(name = "KnnParameters",
        description = "Tuning parameters for the HNSW algorithm within the selected k-NN engine. " +
                      "These settings allow for fine-grained control over the trade-off between search accuracy (recall), " +
                      "indexing speed, and memory usage.")
public record KnnParameters(
    @JsonProperty("m")
    @Schema(description = "The maximum number of connections each node in the HNSW graph can have. This parameter is fundamental to the graph's structure. " +
                      "A higher value for 'm' creates a denser graph with more pathways, which can significantly improve search accuracy and recall, especially for complex datasets. " +
                      "However, this improvement comes at a direct cost: doubling 'm' will roughly double the memory usage of the index and increase the time required to build it.",
            defaultValue = "16",
            examples = {"16", "32", "64"})
    @Min(1)
    Optional<Integer> m,

    @JsonProperty("ef_construction")
    @Schema(description = "The size of the dynamic list of candidate neighbors used during graph construction (at index time). Think of this as the 'beam width' for building the graph. " +
                      "A larger value allows the algorithm to perform a more exhaustive search for the best neighbors for each new node, resulting in a higher-quality graph and thus better search accuracy. " +
                      "The trade-off is a direct, linear impact on indexing speed; doubling this value will roughly double indexing time.",
            defaultValue = "256",
            examples = {"256", "512", "1024"})
    @Min(1)
    Optional<Integer> efConstruction,

    @JsonProperty("ef_search")
    @Schema(description = "The size of the dynamic list of candidate neighbors used during a search query. This is the most critical query-time parameter for tuning the balance between speed and accuracy. " +
                      "A larger value increases the number of nodes explored from the entry point of the graph, making it much more likely to find the true nearest neighbors (higher recall). " +
                      "This directly increases search latency, as more distance calculations are performed. It is recommended to tune this parameter based on your specific application's recall and latency requirements.",
            defaultValue = "512",
            examples = {"256", "512", "2048"})
    @Min(1)
    Optional<Integer> efSearch
) {}