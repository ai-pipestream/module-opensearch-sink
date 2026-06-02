package ai.pipestream.module.opensearchsink;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.module.opensearchsink.service.RepoClient;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocResponse;
import ai.pipestream.repository.v1.IndexingRequestEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import java.nio.charset.StandardCharsets;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Producer-side of the decoupled OpenSearch indexing pipeline. Builds the
 * exact {@link StreamIndexDocumentsRequest} the sink used to send via the
 * manager's bidi {@code StreamIndexDocuments} RPC. Saves this payload to S3
 * via {@link RepoClient#savePipeDoc} on drive {@code {accountId}:index:{indexName}}
 * and then emits an {@link IndexingRequestEvent} referencing it via Kafka.
 */
@ApplicationScoped
public class OpenSearchIndexingPublisher {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingPublisher.class);

    @Inject
    RepoClient repoClient;

    @Inject
    @ProtobufChannel("indexing-requests-out")
    ProtobufEmitter<IndexingRequestEvent> emitter;

    @Inject
    ChunkDocumentConverter chunkDocumentConverter;

    /**
     * Payloads with a serialized size at or below this byte count travel
     * INLINE on the IndexingRequestEvent (no SavePipeDoc / S3 round trip).
     * Anything larger uses the claim-check path. Default 262144 bytes
     * (256 KiB) leaves plenty of headroom under Kafka's 1 MiB default
     * {@code message.max.bytes} after envelope + headers overhead.
     * Operators can raise it once they've raised Kafka's broker limit.
     */
    @ConfigProperty(name = "opensearch-sink.indexing.inline-threshold-bytes", defaultValue = "262144")
    int inlineThresholdBytes;

    /**
     * Build the {@link StreamIndexDocumentsRequest} for {@code (plan, document)}
     * and publish it to the manager via Kafka.
     *
     * <p>Transport is chosen by the serialized size of the request:
     * <ul>
     *   <li><b>≤ {@link #inlineThresholdBytes}</b> — inline. Packed as
     *       {@code google.protobuf.Any} directly on
     *       {@link IndexingRequestEvent#getInlinePayload()}. No S3
     *       round-trip on either side.</li>
     *   <li><b>&gt; threshold</b> — claim-check. The request is
     *       {@code SavePipeDoc}'d to repository-service S3 on drive
     *       {@code {accountId}:index:{indexName}}; only a
     *       {@link DocumentReference} travels on the event.</li>
     * </ul>
     *
     * <p>Both paths stamp {@code crawl_id} on the event envelope so
     * receipt consumers can group / filter by crawl WITHOUT touching
     * the payload. Returns the nodeId of the saved PipeDoc (claim-check
     * path) or the inline event_id (inline path) — caller logs it for
     * audit but doesn't otherwise rely on which transport was chosen.
     */
    public String publish(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc, String crawlId) {
        StreamIndexDocumentsRequest request = buildIndexRequest(plan, document, osDoc, crawlId);

        String accountId = document.hasOwnership() ? document.getOwnership().getAccountId() : "system";
        String datasourceId = document.hasOwnership() ? document.getOwnership().getDatasourceId() : "opensearch-sink";

        // Deterministic event_id from (doc_id, plan_id): if the sink emits
        // the same logical request twice (producer retry, worker-loop
        // redelivery), both events land on the same event_id so consumers
        // / receipt ledger can dedup naturally. Random IDs would have made
        // every retry look like a fresh event.
        String eventId = deterministicUuid("evt:" + document.getDocId() + ":" + plan.id());
        // document_id MUST be on the envelope regardless of transport —
        // the registered UuidKeyExtractor reads it directly to mint the
        // deterministic Kafka partition key without inspecting transport
        // (document_ref vs inline_payload) or unpacking Any.
        IndexingRequestEvent.Builder eventBuilder = IndexingRequestEvent.newBuilder()
                .setEventId(eventId)
                .setDocumentId(document.getDocId())
                .setPlanId(plan.id());
        if (crawlId != null && !crawlId.isEmpty()) {
            eventBuilder.setCrawlId(crawlId);
        }

        int serializedSize = request.getSerializedSize();
        String transport;
        String trace;
        if (serializedSize <= inlineThresholdBytes) {
            // Inline fast path — no SavePipeDoc, no S3 round-trip.
            eventBuilder.setInlinePayload(Any.pack(request));
            transport = "inline";
            trace = "bytes=" + serializedSize;
        } else {
            // Claim-check — too large to inline. Persist to S3 first,
            // then the consumer dereferences on receipt.
            String nodeId = saveToRepository(request, document, accountId, datasourceId, plan.indexName());
            eventBuilder.setDocumentRef(DocumentReference.newBuilder()
                    .setDocId(document.getDocId())
                    .setAccountId(accountId)
                    .setGraphAddressId(datasourceId)
                    .build());
            transport = "claim-check";
            trace = "nodeId=" + nodeId + " bytes=" + serializedSize;
        }

        IndexingRequestEvent event = eventBuilder.build();

        try {
            emitter.send(event).toCompletableFuture().join();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to emit IndexingRequestEvent for doc=%s plan=%s transport=%s",
                    document.getDocId(), plan.id(), transport);
            throw new RuntimeException("Failed to emit IndexingRequestEvent", e);
        }

        LOG.infof("Published indexing request: doc=%s plan=%s index=%s transport=%s eventId=%s %s",
                document.getDocId(), plan.id(), plan.indexName(), transport, eventId, trace);

        // Backward-compat return: caller (the module processor) logs the
        // result but doesn't act on the specific string. Inline emits the
        // event_id; claim-check emits the S3 nodeId.
        return transport.equals("inline") ? eventId : trace.split(" ")[0].substring("nodeId=".length());
    }

    /**
     * Claim-check helper: wrap the request in a PipeDoc, SavePipeDoc to
     * the repository-service S3 storage on drive
     * {@code {accountId}:index:{indexName}}, return the resulting
     * nodeId.
     */
    private String saveToRepository(StreamIndexDocumentsRequest request, PipeDoc document,
                                    String accountId, String datasourceId, String indexName) {
        Any structuredData = Any.pack(request);
        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId(document.getDocId())
                .setStructuredData(structuredData)
                .setOwnership(document.getOwnership())
                .build();
        String driveId = accountId + ":index:" + indexName;
        SavePipeDocRequest saveRequest = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .setDrive(driveId)
                .setConnectorId(datasourceId)
                .setUseDatasourceId(true)
                .build();
        SavePipeDocResponse saveResponse = repoClient.savePipeDoc(saveRequest);
        return saveResponse.getNodeId();
    }

    /**
     * Pure-CPU build of the protobuf request the consumer will eventually act
     * on. Stamps {@code crawl_id} onto the base document and (for chunked
     * strategies) onto every chunk so per-run progress queries can filter.
     */
    @VisibleForTesting
    StreamIndexDocumentsRequest buildIndexRequest(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc, String crawlId) {
        String indexName = plan.indexName();
        OpenSearchDocument stampedOsDoc = (crawlId == null || crawlId.isEmpty())
                ? osDoc
                : osDoc.toBuilder().setCrawlId(crawlId).build();

        // Deterministic request_id from (doc_id, index_name) for the same
        // reason event_id is deterministic: retries / re-publishes must
        // carry the same correlation id so receipts and any consumer-side
        // dedup work as designed. Random would have made every retry
        // look like a brand-new logical request.
        StreamIndexDocumentsRequest.Builder builder = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(deterministicUuid("req:" + document.getDocId() + ":" + indexName))
                .setIndexName(indexName)
                .setDocument(stampedOsDoc)
                .setDocumentId(document.getDocId());

        if (document.hasOwnership()) {
            builder.setAccountId(document.getOwnership().getAccountId());
            builder.setDatasourceId(document.getOwnership().getDatasourceId());
        }

        ai.pipestream.opensearch.v1.IndexingStrategy protoStrategy = plan.strategy();
        if (protoStrategy == null
                || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED) {
            throw new IllegalStateException(
                    "IndexPlan '" + plan.id() + "' returned UNSPECIFIED indexing_strategy on a READY plan — "
                            + "manager contract violation; plan is misconfigured");
        }
        builder.setIndexingStrategy(protoStrategy);

        if (protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED
                || protoStrategy == ai.pipestream.opensearch.v1.IndexingStrategy.INDEXING_STRATEGY_SEPARATE_INDICES) {

            ChunkConversionResult chunkResult = chunkDocumentConverter.convertToChunks(
                    document, stampedOsDoc, indexName, protoStrategy);

            builder.setDocumentMap(chunkResult.documentMap());
            if (crawlId != null && !crawlId.isEmpty()) {
                for (var chunk : chunkResult.chunkDocuments()) {
                    builder.addChunkDocuments(chunk.toBuilder().setCrawlId(crawlId).build());
                }
            } else {
                builder.addAllChunkDocuments(chunkResult.chunkDocuments());
            }

        }

        return builder.build();
    }

    /**
     * Build a name-based UUID (RFC 4122 v3 via {@link UUID#nameUUIDFromBytes})
     * from a stable input string. Same input → same UUID, every time,
     * across processes and restarts. Used for event_id and request_id so
     * that producer retries and worker-loop redeliveries of the same
     * logical indexing request carry the same correlation IDs.
     */
    private static String deterministicUuid(String key) {
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
