package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

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
    SchemaExtractorService schemaExtractorService;

    @Inject
    ObjectMapper objectMapper;

    void onStart(@Observes StartupEvent ev) {
        LOG.info("OpenSearch Ingestion Service starting...");
    }

    @Override
    public Multi<StreamDocumentsResponse> streamDocuments(Multi<StreamDocumentsRequest> requestStream) {
        // NOTE: StreamDocumentsRequest currently lacks a config field in the proto.
        // We fall back to legacy behavior (document type inference) for the streaming path.
        return requestStream.onItem().transformToUniAndMerge(req -> processSingleRequest(req, Optional.empty()));
    }

    private Uni<StreamDocumentsResponse> processSingleRequest(StreamDocumentsRequest request, Optional<OpenSearchSinkOptions> options) {
        if (!request.hasDocument()) {
            return Uni.createFrom().item(buildResponse(request, false, "StreamDocumentsRequest has no document."));
        }

        // Use index name from Engine options if provided, otherwise fallback to document type inference
        String indexName = options.map(OpenSearchSinkOptions::indexName)
                .orElseGet(() -> schemaManager.determineIndexName(request.getDocument().getSearchMetadata().getDocumentType()));

        return schemaManager.indexDocumentViaManager(indexName, request.getDocument(), options)
                .onItem().transform(managerMessage -> {
                    LOG.infof("Successfully indexed document %s into %s via manager", request.getDocument().getDocId(), indexName);
                    return buildResponse(request, true, managerMessage);
                })
                .onFailure().recoverWithUni(error -> {
                    LOG.errorf(error, "Failed to process document %s via manager", request.getDocument().getDocId());
                    return Uni.createFrom().item(buildResponse(request, false, "Indexing failed: " + error.getMessage()));
                });
    }

    private StreamDocumentsResponse buildResponse(StreamDocumentsRequest request, boolean success, String message) {
        String docId = request.hasDocument() ? request.getDocument().getDocId() : "";
        return StreamDocumentsResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setDocumentId(docId)
                .setSuccess(success)
                .setMessage(message)
                .build();
    }

    @Override
    public Uni<ProcessDataResponse> processData(ProcessDataRequest request) {
        LOG.info("ProcessData called for OpenSearch sink module");

        if (!request.hasDocument()) {
            return Uni.createFrom().item(ProcessDataResponse.newBuilder()
                .setSuccess(false)
                .addProcessorLogs("No document provided in request")
                .build());
        }

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
                        .setSuccess(false)
                        .addProcessorLogs("Invalid configuration: " + e.getMessage())
                        .build());
            }
        }

        // 2. Convert to ingestion request and process with options
        StreamDocumentsRequest streamRequest = StreamDocumentsRequest.newBuilder()
            .setRequestId(java.util.UUID.randomUUID().toString())
            .setDocument(request.getDocument())
            .build();

        return processSingleRequest(streamRequest, options)
            .map(streamResponse -> ProcessDataResponse.newBuilder()
                .setSuccess(streamResponse.getSuccess())
                .addProcessorLogs(streamResponse.getMessage())
                .setOutputDoc(request.getDocument())
                .build());
    }

    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.info("GetServiceRegistration called for OpenSearch sink module");

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("opensearch-sink")
                .setVersion("0.1.2-SNAPSHOT")
                .setDisplayName("OpenSearch Sink")
                .setDescription("OpenSearch vector indexing sink with organic schema management")
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("OpenSearch sink module is healthy");

        schemaExtractorService.extractConfigSchemaResolvedForJsonForms()
                .ifPresent(responseBuilder::setJsonConfigSchema);

        return Uni.createFrom().item(responseBuilder.build());
    }
}
