package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.LogEntrySource;
import ai.pipestream.data.v1.LogLevel;
import ai.pipestream.data.v1.ModuleLogOrigin;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.plan.IndexPlanCache;
import ai.pipestream.module.opensearchsink.plan.PlanResolutionException;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import ai.pipestream.module.opensearchsink.service.ConversionResult;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import ai.pipestream.server.meta.BuildInfoProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC service implementation for the OpenSearch sink's production
 * {@code PipeStepProcessor} entrypoint used by the engine. Handles
 * {@code processData} (index a single document via every IndexPlan referenced
 * in the sink config) and {@code getServiceRegistration} (advertise the JSON
 * config schema to the registry).
 *
 * <p>Split out of {@link OpenSearchIngestionServiceImpl} because the gRPC
 * generator emits a separate abstract base class per service and Java does
 * not allow multi-extension. Both classes share {@link SchemaManagerService}
 * and {@link IndexPlanCache} via CDI.
 */
@Singleton
@GrpcService
@RegisterForReflection
public class OpenSearchPipeStepProcessorImpl
        extends PipeStepProcessorServiceGrpc.PipeStepProcessorServiceImplBase {

    private static final Logger LOG = Logger.getLogger(OpenSearchPipeStepProcessorImpl.class);

    @Inject
    OpenSearchIndexingPublisher indexingPublisher;

    @Inject
    DocumentConverterService documentConverter;

    @Inject
    SchemaExtractorService schemaExtractorService;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    BuildInfoProvider buildInfoProvider;

    @Inject
    IndexPlanCache indexPlanCache;

    @Override
    @RunOnVirtualThread
    public void processData(ProcessDataRequest request, StreamObserver<ProcessDataResponse> responseObserver) {
        long startTime = System.currentTimeMillis();
        List<LogEntry> auditLogs = new ArrayList<>();
        LOG.info("ProcessData called for OpenSearch sink module");

        if (!request.hasDocument()) {
            responseObserver.onNext(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addLogEntries(moduleLog("No document provided in request", LogLevel.LOG_LEVEL_ERROR))
                    .build());
            responseObserver.onCompleted();
            return;
        }

        String docId = request.getDocument().getDocId();

        // 1. Extract and parse JSON configuration provided by the Engine.
        // The sink REQUIRES a config carrying plan_ids — there is no
        // "infer index name from document type" fallback now that plans
        // are the single source of routing truth.
        OpenSearchSinkOptions options;
        if (!request.hasConfig() || !request.getConfig().hasJsonConfig()) {
            responseObserver.onNext(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addLogEntries(moduleLog(
                            "OpenSearch sink requires JSON config carrying plan_ids; none was provided on the request",
                            LogLevel.LOG_LEVEL_ERROR))
                    .build());
            responseObserver.onCompleted();
            return;
        }
        try {
            String json = JsonFormat.printer().print(request.getConfig().getJsonConfig());
            options = objectMapper.readValue(json, OpenSearchSinkOptions.class);
        } catch (Exception e) {
            LOG.error("Failed to parse Sink configuration from request", e);
            responseObserver.onNext(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addLogEntries(moduleLog("Invalid configuration: " + e.getMessage(), LogLevel.LOG_LEVEL_ERROR))
                    .build());
            responseObserver.onCompleted();
            return;
        }

        // 2. Resolve plan_ids → ResolvedPlan snapshots. This is the sink's
        // "startup verification" path, run per request: every plan must
        // exist and be READY or the request fails loud with a message
        // naming each offender.
        List<ResolvedPlan> plans;
        try {
            plans = indexPlanCache.resolve(options.planIdsOrEmpty());
        } catch (PlanResolutionException error) {
            LOG.errorf(error, "Plan resolution failed for docId=%s", docId);
            auditLogs.add(moduleLog(
                    "Plan resolution failed: " + error.getMessage(),
                    LogLevel.LOG_LEVEL_ERROR));
            responseObserver.onNext(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addAllLogEntries(auditLogs)
                    .setOutputDoc(request.getDocument())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        // 3. Convert document once — reused for both audit trail and indexing
        ConversionResult conversionResult = documentConverter.convertWithAuditLog(request.getDocument());
        conversionResult.auditLogs().forEach(msg -> auditLogs.add(moduleLog(msg, LogLevel.LOG_LEVEL_INFO)));

        String crawlId = request.hasMetadata() ? request.getMetadata().getCrawlId() : "";

        // 4. Sequential fan-out: one XADD per plan to that plan's redis
        // indexing stream (pipestream:indexing:<plan_id>). The sink's role
        // is now produce-only — opensearch-manager consumes these streams
        // and performs the actual OpenSearch bulk write asynchronously. A
        // failure on any plan's XADD fails the whole request because a
        // doc enqueued for N-1 plans and dropped on the Nth would create
        // silent across-index inconsistency once the manager drains.
        //
        // HANDOFF success at this node means "request shaped and queued";
        // end-to-end OpenSearch durability comes back via the receipt
        // event the manager will emit on bulk-ACK (Phase 2).
        try {
            for (ResolvedPlan plan : plans) {
                auditLogs.add(moduleLog(
                        "Queueing document " + docId + " via plan " + plan.id()
                                + " for index '" + plan.indexName()
                                + "' with strategy " + plan.strategy().name(),
                        LogLevel.LOG_LEVEL_INFO));
                indexingPublisher.publish(
                        plan, request.getDocument(), conversionResult.document(), crawlId);
            }
        } catch (RuntimeException error) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.errorf(error, "Failed to queue document %s across plans", docId);
            auditLogs.add(moduleLog(
                    "Document queue-for-indexing failed after " + duration + "ms: " + error.getMessage(),
                    LogLevel.LOG_LEVEL_ERROR));
            responseObserver.onNext(ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addAllLogEntries(auditLogs)
                    .setOutputDoc(request.getDocument())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        long duration = System.currentTimeMillis() - startTime;
        LOG.infof("OpenSearch sink queued docId=%s across %d plan(s) in %dms",
                docId, plans.size(), duration);
        auditLogs.add(moduleLog(
                "Document queued for indexing across " + plans.size()
                        + " plan(s) in " + duration + "ms",
                LogLevel.LOG_LEVEL_INFO));
        responseObserver.onNext(ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                .addAllLogEntries(auditLogs)
                .setOutputDoc(request.getDocument())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    @RunOnVirtualThread
    public void getServiceRegistration(GetServiceRegistrationRequest request,
                                       StreamObserver<GetServiceRegistrationResponse> responseObserver) {
        LOG.info("GetServiceRegistration called for OpenSearch sink module");

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("opensearch-sink")
                .setVersion(buildInfoProvider.getVersion())
                .setDisplayName("OpenSearch Sink")
                .setDescription("OpenSearch vector indexing sink with organic schema management. Standard pipeline execution uses processData; StreamDocuments is experimental.")
                .putAllMetadata(buildInfoProvider.registrationMetadata())
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("OpenSearch sink module is healthy");

        schemaExtractorService.extractConfigSchemaResolvedForJsonForms()
                .ifPresent(responseBuilder::setJsonConfigSchema);

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private static LogEntry moduleLog(String message, LogLevel level) {
        return LogEntry.newBuilder()
                .setSource(LogEntrySource.LOG_ENTRY_SOURCE_MODULE)
                .setLevel(level)
                .setMessage(message)
                .setTimestampEpochMs(System.currentTimeMillis())
                .setModule(ModuleLogOrigin.newBuilder().setModuleName("opensearch-sink").build())
                .build();
    }
}
