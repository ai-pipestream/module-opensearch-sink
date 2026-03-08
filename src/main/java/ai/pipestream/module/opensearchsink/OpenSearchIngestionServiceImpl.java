package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * The canonical OpenSearch ingestion service and PipeStepProcessor implementation.
 * This service delegates all indexing and schema management to the OpenSearch Manager.
 */
@GrpcService
public class OpenSearchIngestionServiceImpl
        extends MutinyOpenSearchIngestionServiceGrpc.OpenSearchIngestionServiceImplBase
        implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIngestionServiceImpl.class);

    private final SchemaManagerService schemaManager;
    private final SchemaExtractorService schemaExtractorService;

    @Inject
    public OpenSearchIngestionServiceImpl(
            SchemaManagerService schemaManager,
            SchemaExtractorService schemaExtractorService) {
        this.schemaManager = schemaManager;
        this.schemaExtractorService = schemaExtractorService;
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
                .onItem().transform(v -> {
                    LOG.infof("Successfully indexed document %s via manager", request.getDocument().getDocId());
                    return buildResponse(request, true, "Document indexed successfully via manager.");
                })
                .onFailure().recoverWithUni(error -> {
                    LOG.errorf(error, "Failed to process document %s via manager", request.getDocument().getDocId());
                    return Uni.createFrom().item(buildResponse(request, false, "Processing failed: " + error.getMessage()));
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

    @RunOnVirtualThread
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

    @RunOnVirtualThread
    @Override
    public Uni<GetServiceRegistrationResponse> getServiceRegistration(GetServiceRegistrationRequest request) {
        LOG.info("OpenSearch sink service registration requested");

        Capabilities capabilities = Capabilities.newBuilder()
                .addTypes(CapabilityType.CAPABILITY_TYPE_SINK)
                .build();

        GetServiceRegistrationResponse.Builder responseBuilder = GetServiceRegistrationResponse.newBuilder()
                .setModuleName("opensearch-sink")
                .setVersion("1.0.0-SNAPSHOT")
                .setDisplayName("OpenSearch Sink")
                .setDescription("OpenSearch vector indexing sink with organic schema management via opensearch-manager")
                .addTags("opensearch")
                .addTags("sink")
                .addTags("vector")
                .addTags("indexing")
                .addTags("module")
                .setCapabilities(capabilities)
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("OpenSearch sink module is healthy");

        schemaExtractorService.extractConfigSchemaResolvedForJsonForms()
                .ifPresent(responseBuilder::setJsonConfigSchema);

        return Uni.createFrom().item(responseBuilder.build());
    }
}
