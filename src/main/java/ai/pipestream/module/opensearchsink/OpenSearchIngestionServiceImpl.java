package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.MutinyOpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import ai.pipestream.module.opensearchsink.service.OpenSearchRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * The canonical OpenSearch ingestion service and PipeStepProcessor implementation.
 * This service handles both:
 * 1. The gRPC OpenSearchIngestion.streamDocuments bidirectional stream
 * 2. The PipeStepProcessorService interface for pipeline module integration
 *
 * All document processing flows through DocumentConverterService, which is the
 * single source of truth for PipeDoc -> OpenSearchDocument conversion.
 */
@GrpcService
public class OpenSearchIngestionServiceImpl
        extends MutinyOpenSearchIngestionServiceGrpc.OpenSearchIngestionServiceImplBase
        implements PipeStepProcessorService {

    private static final Logger LOG = Logger.getLogger(OpenSearchIngestionServiceImpl.class);

    private final SchemaManagerService schemaManager;
    private final DocumentConverterService documentConverter;
    private final OpenSearchRepository openSearchRepository;

    @Inject
    public OpenSearchIngestionServiceImpl(
            SchemaManagerService schemaManager,
            DocumentConverterService documentConverter,
            OpenSearchRepository openSearchRepository) {
        this.schemaManager = schemaManager;
        this.documentConverter = documentConverter;
        this.openSearchRepository = openSearchRepository;
    }

    @Override
    public Multi<StreamDocumentsResponse> streamDocuments(Multi<StreamDocumentsRequest> requestStream) {
        // For now, we process each request individually.
        // We will add buffering logic later based on the configuration.
        return requestStream.onItem().transformToUniAndMerge(this::processSingleRequest);
    }

    private Uni<StreamDocumentsResponse> processSingleRequest(StreamDocumentsRequest request) {
        if (!request.hasDocument()) {
            return Uni.createFrom().item(buildResponse(request, false, "StreamDocumentsRequest has no document."));
        }

        // Extract document type from PipeDoc.search_metadata
        String documentType = request.getDocument().getSearchMetadata().getDocumentType();
        String indexName = schemaManager.determineIndexName(documentType);

        return schemaManager.ensureIndexExists(indexName, request.getDocument())
                .onItem().transform(v -> documentConverter.prepareBulkRequest(request.getDocument(), indexName))
                .onItem().transformToUni(openSearchRepository::bulk)
                .onItem().transform(bulkResponse -> {
                    if (bulkResponse.getErrors()) {
                        String errDetail = bulkResponse.getItemsList().stream()
                                .filter(i -> (i.hasIndex() && i.getIndex().hasError()) || (i.hasCreate() && i.getCreate().hasError()))
                                .map(i -> {
                                    if (i.hasIndex() && i.getIndex().hasError()) {
                                        var e = i.getIndex().getError();
                                        return e.getType() + ": " + e.getReason();
                                    }
                                    if (i.hasCreate() && i.getCreate().hasError()) {
                                        var e = i.getCreate().getError();
                                        return e.getType() + ": " + e.getReason();
                                    }
                                    return "unknown";
                                })
                                .findFirst()
                                .orElse("unknown");
                        LOG.warnf("Bulk gRPC request had errors for document %s: %s", request.getDocument().getDocId(), errDetail);
                        return buildResponse(request, false, "Bulk operation completed with errors: " + errDetail);
                    } else {
                        LOG.infof("Successfully indexed document %s", request.getDocument().getDocId());
                        return buildResponse(request, true, "Document indexed successfully.");
                    }
                })
                .onFailure().recoverWithItem(error -> {
                    LOG.errorf(error, "Failed to process document %s", request.getDocument().getDocId());
                    return buildResponse(request, false, "Processing failed: " + error.getMessage());
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

    // PipeStepProcessorService interface implementation
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
                .setDescription("OpenSearch vector indexing sink with dynamic schema creation and distributed locking")
                .addTags("opensearch")
                .addTags("sink")
                .addTags("vector")
                .addTags("indexing")
                .addTags("module")
                .setCapabilities(capabilities)
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("OpenSearch sink module is healthy");

        return Uni.createFrom().item(responseBuilder.build());
    }
}
