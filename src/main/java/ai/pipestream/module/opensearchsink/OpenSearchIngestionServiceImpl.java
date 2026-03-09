package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.data.module.v1.PipeStepProcessorService;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.quarkus.vertx.ConsumeEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

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

    void onStart(@Observes StartupEvent ev) {
        LOG.info("OpenSearch Ingestion Service starting...");
    }

    @Override
    public Multi<StreamDocumentsResponse> streamDocuments(Multi<StreamDocumentsRequest> requestStream) {
        return requestStream.onItem().transformToUniAndMerge(this::processSingleRequest);
    }

    private Uni<StreamDocumentsResponse> processSingleRequest(StreamDocumentsRequest request) {
        if (!request.hasDocument()) {
            return Uni.createFrom().item(buildResponse(request, false, "StreamDocumentsRequest has no document."));
        }

        // Extract document type from PipeDoc.search_metadata
        String documentType = request.getDocument().getSearchMetadata().getDocumentType();
        String indexName = schemaManager.determineIndexName(documentType);

        return schemaManager.indexDocumentViaManager(indexName, request.getDocument())
                .onItem().transform(managerMessage -> {
                    LOG.infof("Successfully indexed document %s via manager", request.getDocument().getDocId());
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

        // Convert to ingestion request and process
        StreamDocumentsRequest streamRequest = StreamDocumentsRequest.newBuilder()
            .setRequestId(java.util.UUID.randomUUID().toString())
            .setDocument(request.getDocument())
            .build();

        return processSingleRequest(streamRequest)
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
