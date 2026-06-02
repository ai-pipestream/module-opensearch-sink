package ai.pipestream.module.opensearchsink.support;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;

/** Builds {@link PipeStream} fixtures for demand-pull processor tests. */
public final class PipeStreamTestSupport {

    private PipeStreamTestSupport() {}

    public static PipeStream withSinkConfig(PipeDoc document, String opensearchInstance, String... planIds)
            throws JsonProcessingException {
        return withSinkConfig(document, opensearchInstance, Arrays.asList(planIds));
    }

    public static PipeStream withSinkConfig(PipeDoc document, String opensearchInstance, List<String> planIds)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String planIdsJson = mapper.writeValueAsString(planIds);
        StreamMetadata metadata = StreamMetadata.newBuilder()
                .putContextParams("opensearch_instance", opensearchInstance)
                .putContextParams("plan_ids", planIdsJson)
                .build();
        return PipeStream.newBuilder()
                .setStreamId("test-stream")
                .setCurrentNodeId("opensearch-sink")
                .setDocument(document)
                .setMetadata(metadata)
                .build();
    }
}
