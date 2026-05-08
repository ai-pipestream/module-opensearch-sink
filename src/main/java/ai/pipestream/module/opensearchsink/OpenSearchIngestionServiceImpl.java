package ai.pipestream.module.opensearchsink;

import ai.pipestream.ingestion.v1.OpenSearchIngestionServiceGrpc;
import ai.pipestream.ingestion.v1.StreamDocumentsRequest;
import ai.pipestream.ingestion.v1.StreamDocumentsResponse;
import ai.pipestream.module.opensearchsink.service.DocumentConverterService;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * gRPC service implementation for the OpenSearch sink's experimental direct
 * ingestion stream. Distinct from {@link OpenSearchPipeStepProcessorImpl},
 * which handles the production module-step entrypoint used by the engine.
 *
 * <p>{@code streamDocuments} is a bidi stream of documents direct from a
 * loader into the sink. The sink batches them into micro-batches of 100,
 * provisions every distinct (index, semantic-config-set) once per batch,
 * then opens one bidi stream to the manager per micro-batch via
 * {@link SchemaManagerService#streamIndexDocumentsViaManager(List)} to push
 * the documents downstream.
 */
@Singleton
@GrpcService
@RegisterForReflection
public class OpenSearchIngestionServiceImpl
        extends OpenSearchIngestionServiceGrpc.OpenSearchIngestionServiceImplBase {

    private static final Logger LOG = Logger.getLogger(OpenSearchIngestionServiceImpl.class);
    private static final int BATCH_SIZE = 100;

    @Inject
    SchemaManagerService schemaManager;

    @Inject
    DocumentConverterService documentConverter;

    void onStart(@Observes StartupEvent ev) {
        LOG.info("OpenSearch Ingestion Service starting...");
    }

    /**
     * Plain async-callback bidi handler. The inbound observer accumulates
     * requests into a 100-element micro-batch buffer and flushes each full
     * batch (and the trailing partial batch on close) on a virtual thread:
     * provisioning + manager bidi stream are blocking calls.
     */
    @Override
    @RunOnVirtualThread
    public StreamObserver<StreamDocumentsRequest> streamDocuments(
            StreamObserver<StreamDocumentsResponse> responseObserver) {
        LOG.info("Experimental StreamDocuments called for OpenSearch sink module");

        return new StreamObserver<>() {
            private final List<StreamDocumentsRequest> buffer = new ArrayList<>();

            @Override
            public synchronized void onNext(StreamDocumentsRequest request) {
                buffer.add(request);
                if (buffer.size() >= BATCH_SIZE) {
                    List<StreamDocumentsRequest> batch = new ArrayList<>(buffer);
                    buffer.clear();
                    flush(batch, responseObserver);
                }
            }

            @Override
            public synchronized void onError(Throwable t) {
                LOG.error("Inbound StreamDocuments stream errored", t);
                responseObserver.onError(t);
            }

            @Override
            public synchronized void onCompleted() {
                if (!buffer.isEmpty()) {
                    List<StreamDocumentsRequest> batch = new ArrayList<>(buffer);
                    buffer.clear();
                    flush(batch, responseObserver);
                }
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Provision every distinct (index, semantic-config-set) in the batch,
     * stream the batch to the manager, and forward correlated responses to
     * the caller. Plain blocking on a virtual thread.
     */
    void flush(List<StreamDocumentsRequest> batch, StreamObserver<StreamDocumentsResponse> responseObserver) {
        if (batch.isEmpty()) return;
        try {
            provisionBatch(batch);
        } catch (RuntimeException provisioningFailure) {
            String message = provisioningFailure.getMessage() != null
                    ? provisioningFailure.getMessage()
                    : provisioningFailure.toString();
            for (StreamDocumentsRequest req : batch) {
                responseObserver.onNext(StreamDocumentsResponse.newBuilder()
                        .setRequestId(req.getRequestId())
                        .setDocumentId(req.getDocument().getDocId())
                        .setSuccess(false)
                        .setMessage("Indexing failed: " + message)
                        .build());
            }
            return;
        }

        List<ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest> managerRequests =
                new ArrayList<>(batch.size());
        for (StreamDocumentsRequest req : batch) {
            managerRequests.add(toManagerRequest(req));
        }

        List<ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse> managerResponses;
        try {
            managerResponses = schemaManager.streamIndexDocumentsViaManager(managerRequests);
        } catch (RuntimeException streamFailure) {
            LOG.error("Stream to manager failed", streamFailure);
            String message = streamFailure.getMessage() != null
                    ? streamFailure.getMessage()
                    : streamFailure.toString();
            for (StreamDocumentsRequest req : batch) {
                responseObserver.onNext(StreamDocumentsResponse.newBuilder()
                        .setRequestId(req.getRequestId())
                        .setDocumentId(req.getDocument().getDocId())
                        .setSuccess(false)
                        .setMessage("Indexing failed: " + message)
                        .build());
            }
            return;
        }

        for (ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse resp : managerResponses) {
            String forward = resp.getSuccess() ? resp.getMessage() : "Indexing failed: " + resp.getMessage();
            responseObserver.onNext(StreamDocumentsResponse.newBuilder()
                    .setRequestId(resp.getRequestId())
                    .setDocumentId(resp.getDocumentId())
                    .setSuccess(resp.getSuccess())
                    .setMessage(forward)
                    .build());
        }
    }

    private void provisionBatch(List<StreamDocumentsRequest> batch) {
        Map<String, ProvisioningTarget> targets = new LinkedHashMap<>();
        for (StreamDocumentsRequest req : batch) {
            String documentType = documentType(req);
            String indexName = schemaManager.determineIndexName(documentType);
            String cacheKey = schemaManager.provisioningCacheKey(indexName, req.getDocument());
            if (cacheKey != null) {
                targets.putIfAbsent(cacheKey, new ProvisioningTarget(indexName, req.getDocument(), documentType));
            }
        }
        for (ProvisioningTarget target : targets.values()) {
            schemaManager.ensureIndexProvisioned(
                    target.indexName(), target.document(), target.documentType());
        }
    }

    private ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest toManagerRequest(StreamDocumentsRequest req) {
        String docType = documentType(req);
        String indexName = schemaManager.determineIndexName(docType);
        ai.pipestream.opensearch.v1.OpenSearchDocument osDoc =
                documentConverter.convertToOpenSearchDocument(req.getDocument());

        LOG.debugf("Streaming document %s to manager index %s", req.getDocument().getDocId(), indexName);

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
    }

    private static String documentType(StreamDocumentsRequest req) {
        return req.getDocument().hasSearchMetadata()
                ? req.getDocument().getSearchMetadata().getDocumentType()
                : null;
    }

    private record ProvisioningTarget(String indexName, ai.pipestream.data.v1.PipeDoc document, String documentType) {}
}
