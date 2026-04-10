package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.data.v1.LogEntry;
import ai.pipestream.data.v1.LogEntrySource;
import ai.pipestream.data.v1.LogLevel;
import ai.pipestream.data.v1.ModuleLogOrigin;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import ai.pipestream.server.meta.BuildInfoProvider;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import ai.pipestream.module.opensearchsink.service.ConversionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * gRPC service implementation for the OpenSearch sink.
 * Handles both the ingestion stream and standard pipeline step processing.
 */
@Singleton
@GrpcService
@RegisterForReflection
public class OpenSearchIngestionServiceImpl extends MutinyOpenSearchIngestionServiceGrpc.OpenSearchIngestionServiceImplBase implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIngestionServiceImpl.class);

    @Inject
    SchemaManagerService schemaManager;

    @Inject
    ai.pipestream.module.opensearchsink.service.DocumentConverterService documentConverter;

    @Inject
    SchemaExtractorService schemaExtractorService;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    BuildInfoProvider buildInfoProvider;

    void onStart(@Observes StartupEvent ev) {
        LOG.info("OpenSearch Ingestion Service starting...");
    }

    @Override
    public Multi<StreamDocumentsResponse> streamDocuments(Multi<StreamDocumentsRequest> requestStream) {
        LOG.info("StreamDocuments called for OpenSearch sink module");
        // High-throughput path: stream directly to manager
        Multi<ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest> managerRequests = requestStream.onItem().transform(req -> {
            try {
                String documentType = req.getDocument().getSearchMetadata().getDocumentType();
                String indexName = schemaManager.determineIndexName(documentType);
                
                LOG.debugf("Streaming document %s to manager for index %s", req.getDocument().getDocId(), indexName);
                
                ai.pipestream.opensearch.v1.OpenSearchDocument osDoc = documentConverter.convertToOpenSearchDocument(req.getDocument());
                
                var builder = ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest.newBuilder()
                        .setRequestId(req.getRequestId())
                        .setIndexName(indexName)
                        .setDocument(osDoc)
                        .setDocumentId(req.getDocument().getDocId());
                
                if (req.getDocument().hasOwnership()) {
                    builder.setAccountId(req.getDocument().getOwnership().getAccountId());
                    builder.setDatasourceId(req.getDocument().getOwnership().getDatasourceId());
                }
                
                return builder.build();
            } catch (Exception e) {
                LOG.error("Failed to transform StreamDocumentsRequest for manager", e);
                throw e;
            }
        });

        return schemaManager.streamIndexDocumentsViaManager(managerRequests)
                .onItem().transform(resp -> {
                    LOG.debugf("Received streaming response from manager for req: %s, success: %b", resp.getRequestId(), resp.getSuccess());
                    String message = resp.getSuccess() ? resp.getMessage() : "Indexing failed: " + resp.getMessage();
                    return StreamDocumentsResponse.newBuilder()
                        .setRequestId(resp.getRequestId())
                        .setDocumentId(resp.getDocumentId())
                        .setSuccess(resp.getSuccess())
                        .setMessage(message)
                        .build();
                })
                .onFailure().invoke(t -> LOG.error("Stream to manager failed", t));
    }

    @Override
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        long startTime = System.currentTimeMillis();
        List<LogEntry> auditLogs = new ArrayList<>();
        LOG.info("ProcessData called for OpenSearch sink module");

        if (!request.hasDocument()) {
            return Uni.createFrom().item(ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                .addLogEntries(moduleLog("No document provided in request", LogLevel.LOG_LEVEL_ERROR))
                .build());
        }

        String docId = request.getDocument().getDocId();

        // 1. Extract and parse JSON configuration provided by the Engine
        Optional<OpenSearchSinkOptions> options = Optional.empty();
        if (request.hasConfig() && request.getConfig().hasJsonConfig()) {
            try {
                String json = JsonFormat.printer().print(request.getConfig().getJsonConfig());
                options = Optional.of(objectMapper.readValue(json, OpenSearchSinkOptions.class));
                LOG.debugf("Parsed request configuration for index: %s", options.get().indexName());
            } catch (Exception e) {
                LOG.error("Failed to parse Sink configuration from request", e);
                return Uni.createFrom().item(ProcessDataResponse.newBuilder()
                        .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                        .addLogEntries(moduleLog("Invalid configuration: " + e.getMessage(), LogLevel.LOG_LEVEL_ERROR))
                        .build());
            }
        }

        // 2. Convert document once — reused for both audit trail and indexing
        ConversionResult conversionResult = documentConverter.convertWithAuditLog(request.getDocument());
        conversionResult.auditLogs().forEach(msg -> auditLogs.add(moduleLog(msg, LogLevel.LOG_LEVEL_INFO)));

        // 3. Determine index name and log strategy
        String indexName = options.map(OpenSearchSinkOptions::indexName)
                .orElseGet(() -> schemaManager.determineIndexName(
                        request.getDocument().getSearchMetadata().getDocumentType()));
        String strategyName = options.map(o -> o.indexingStrategy() != null ? o.indexingStrategy().name() : "NESTED")
                .orElse("NESTED");
        auditLogs.add(moduleLog("Indexing document " + docId + " to collection '" + indexName
                + "' with strategy " + strategyName, LogLevel.LOG_LEVEL_INFO));

        // 4. Index via manager — pass the already-converted OpenSearchDocument (no double conversion)
        return schemaManager.indexDocumentViaManager(indexName, request.getDocument(), conversionResult.document(), options)
            .map(managerMessage -> {
                long duration = System.currentTimeMillis() - startTime;
                LOG.infof("OpenSearch sink indexed docId=%s index=%s in %dms", docId, indexName, duration);
                auditLogs.add(moduleLog("Document indexed successfully in " + duration + "ms", LogLevel.LOG_LEVEL_INFO));
                return ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                    .addAllLogEntries(auditLogs)
                    .setOutputDoc(request.getDocument())
                    .build();
            })
            .onFailure().recoverWithItem(error -> {
                long duration = System.currentTimeMillis() - startTime;
                LOG.errorf(error, "Failed to index document %s via manager", docId);
                auditLogs.add(moduleLog("Document indexing failed after " + duration + "ms: " + error.getMessage(), LogLevel.LOG_LEVEL_ERROR));
                return ProcessDataResponse.newBuilder()
                    .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_FAILURE)
                    .addAllLogEntries(auditLogs)
                    .setOutputDoc(request.getDocument())
                    .build();
            });
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

    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.info("GetServiceRegistration called for OpenSearch sink module");

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("opensearch-sink")
                .setVersion(buildInfoProvider.getVersion())
                .setDisplayName("OpenSearch Sink")
                .setDescription("OpenSearch vector indexing sink with organic schema management")
                .putAllMetadata(buildInfoProvider.registrationMetadata())
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("OpenSearch sink module is healthy");

        schemaExtractorService.extractConfigSchemaResolvedForJsonForms()
                .ifPresent(responseBuilder::setJsonConfigSchema);

        return Uni.createFrom().item(responseBuilder.build());
    }

}
