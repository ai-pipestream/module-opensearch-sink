package ai.pipestream.module.opensearchsink.work;

import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.server.work.ModuleProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenSearchSinkOptionsParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesJsonArrayPlanIds() {
        OpenSearchSinkOptions options = OpenSearchSinkOptionsParser.parse(
                Map.of(
                        "opensearch_instance", "prod",
                        "plan_ids", "[\"plan-a\",\"plan-b\"]"),
                objectMapper,
                "sink-1");

        assertThat(options.opensearchInstance()).isEqualTo("prod");
        assertThat(options.planIdsOrEmpty()).containsExactly("plan-a", "plan-b");
    }

    @Test
    void rejectsMissingPlanIds() {
        assertThatThrownBy(() -> OpenSearchSinkOptionsParser.parse(
                Map.of("opensearch_instance", "prod"),
                objectMapper,
                "sink-1"))
                .isInstanceOf(ModuleProcessor.PermanentFailure.class)
                .hasMessageContaining("plan_ids");
    }
}
