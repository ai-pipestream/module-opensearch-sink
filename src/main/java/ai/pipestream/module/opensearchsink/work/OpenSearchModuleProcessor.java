package ai.pipestream.module.opensearchsink.work;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.module.opensearchsink.OpenSearchIndexingPublisher;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.plan.IndexPlanCache;
import ai.pipestream.module.opensearchsink.plan.PlanResolutionException;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ConversionResult;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import ai.pipestream.server.work.ModuleProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;

/**
 * Demand-pull processor for the OpenSearch sink. The engine serves
 * {@link PipeStream} work units; this class parses sink options from
 * {@code StreamMetadata.context_params}, resolves {@code plan_ids} via
 * {@link IndexPlanCache}, and enqueues one Redis indexing entry per plan.
 */
@ApplicationScoped
public class OpenSearchModuleProcessor implements ModuleProcessor<PipeStream> {

    private static final Logger LOG = Logger.getLogger(OpenSearchModuleProcessor.class);

    @Inject
    OpenSearchIndexingPublisher indexingPublisher;

    @Inject
    DocumentConverterService documentConverter;

    @Inject
    IndexPlanCache indexPlanCache;

    @Inject
    ObjectMapper objectMapper;

    @Override
    public PipeStream process(PipeStream input) {
        long startTime = System.currentTimeMillis();
        String nodeId = input.getCurrentNodeId().isEmpty() ? "opensearch-sink" : input.getCurrentNodeId();
        String streamId = input.getStreamId();

        if (!input.hasDocument()) {
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink requires an inline PipeDoc on the served PipeStream (stream_id=" + streamId + ")");
        }

        PipeDoc document = input.getDocument();
        String docId = document.getDocId();
        LOG.infof("OpenSearch sink demand-pull RECV stream=%s node=%s doc=%s", streamId, nodeId, docId);

        OpenSearchSinkOptions options;
        try {
            Map<String, String> contextParams = input.hasMetadata()
                    ? input.getMetadata().getContextParamsMap()
                    : Map.of();
            options = OpenSearchSinkOptionsParser.parse(contextParams, objectMapper, nodeId);
        } catch (ModuleProcessor.PermanentFailure e) {
            throw e;
        } catch (RuntimeException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof JsonProcessingException jpe) {
                throw new ModuleProcessor.PermanentFailure(
                        "opensearch-sink node '" + nodeId + "': invalid configuration: " + jpe.getOriginalMessage(),
                        jpe);
            }
            throw new ModuleProcessor.PermanentFailure(
                    "opensearch-sink node '" + nodeId + "': invalid configuration: " + cause.getMessage(), cause);
        }

        List<ResolvedPlan> plans;
        try {
            plans = indexPlanCache.resolve(options.planIdsOrEmpty());
        } catch (PlanResolutionException error) {
            LOG.errorf(error, "Plan resolution failed for docId=%s stream=%s", docId, streamId);
            throw new ModuleProcessor.PermanentFailure(error.getMessage(), error);
        }

        ConversionResult conversionResult = documentConverter.convertWithAuditLog(document);
        conversionResult.auditLogs().forEach(msg ->
                LOG.infof("OpenSearch sink [%s] doc=%s: %s", nodeId, docId, msg));

        String crawlId = input.hasMetadata() && input.getMetadata().hasCrawlId()
                ? input.getMetadata().getCrawlId()
                : "";

        try {
            for (ResolvedPlan plan : plans) {
                LOG.infof("Queueing document %s via plan %s for index '%s' with strategy %s",
                        docId, plan.id(), plan.indexName(), plan.strategy().name());
                indexingPublisher.publish(plan, document, conversionResult.document(), crawlId);
            }
        } catch (RuntimeException error) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.errorf(error, "Failed to queue document %s across plans after %dms", docId, duration);
            throw error;
        }

        long duration = System.currentTimeMillis() - startTime;
        LOG.infof("OpenSearch sink queued docId=%s across %d plan(s) in %dms", docId, plans.size(), duration);
        return input;
    }
}
