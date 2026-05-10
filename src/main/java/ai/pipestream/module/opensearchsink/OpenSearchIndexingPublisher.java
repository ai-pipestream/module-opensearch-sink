package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.XAddArgs;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Producer-side of the redis-decoupled OpenSearch indexing pipeline. Builds the
 * exact {@link StreamIndexDocumentsRequest} the sink used to send via the
 * manager's bidi {@code StreamIndexDocuments} RPC and XADDs it to a per-plan
 * redis stream keyed {@code pipestream:indexing:<plan_id>}. The
 * opensearch-manager will grow a consumer that drains these streams in Phase 2;
 * for now the sink's responsibility ends at "request shaped and on the queue."
 *
 * <p>Stream payload fields per XADD entry:
 * <ul>
 *   <li>{@code job_id} — UUID minted here for traceability</li>
 *   <li>{@code doc_id} — source PipeDoc id (also on the embedded request)</li>
 *   <li>{@code plan_id} — the IndexPlan that produced this request</li>
 *   <li>{@code index_name} — physical OpenSearch index name from the plan</li>
 *   <li>{@code account_id} — present iff PipeDoc had ownership</li>
 *   <li>{@code crawl_id} — empty string when the request didn't carry one</li>
 *   <li>{@code request_payload} — base64 of the {@link StreamIndexDocumentsRequest} proto bytes</li>
 * </ul>
 *
 * <p>Why include doc/plan/index_name as separate fields when they're already
 * inside {@code request_payload}: redis tools (redis-cli XINFO/XRANGE, the
 * upcoming admin UI, any debug script) can read them without parsing protobuf.
 * The {@code request_payload} is what the consumer decodes and acts on.
 */
@ApplicationScoped
public class OpenSearchIndexingPublisher {

    private static final Logger LOG = Logger.getLogger(OpenSearchIndexingPublisher.class);

    static final String STREAM_KEY_PREFIX = "pipestream:indexing:";

    static final String FIELD_JOB_ID = "job_id";
    static final String FIELD_DOC_ID = "doc_id";
    static final String FIELD_PLAN_ID = "plan_id";
    static final String FIELD_INDEX_NAME = "index_name";
    static final String FIELD_ACCOUNT_ID = "account_id";
    static final String FIELD_CRAWL_ID = "crawl_id";
    static final String FIELD_REQUEST_PAYLOAD = "request_payload";

    @Inject
    RedisDataSource redis;

    @Inject
    ChunkDocumentConverter chunkDocumentConverter;

    /**
     * Trim ceiling per stream. Old entries beyond this are dropped by redis on
     * XADD (approximate trimming). Default sized for ~1M docs in-flight; tune
     * per deployment via {@code pipestream.opensearch-sink.indexing.stream-maxlen}.
     */
    @ConfigProperty(name = "pipestream.opensearch-sink.indexing.stream-maxlen", defaultValue = "1000000")
    long streamMaxLen;

    private StreamCommands<String, String, String> streams;

    @PostConstruct
    void init() {
        streams = redis.stream(String.class);
    }

    /**
     * Build the {@link StreamIndexDocumentsRequest} for {@code (plan, document)}
     * and XADD it to {@code pipestream:indexing:<plan_id>}. Returns the redis
     * stream entry id assigned by the broker.
     *
     * <p>An UNSPECIFIED {@link ResolvedPlan#strategy()} is a contract violation
     * (a READY plan must carry a concrete strategy), so it throws rather than
     * silently misrouting docs to a wrong-shape index.
     */
    public String publish(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc, String crawlId) {
        StreamIndexDocumentsRequest request = buildIndexRequest(plan, document, osDoc, crawlId);
        String streamKey = STREAM_KEY_PREFIX + plan.id();
        XAddArgs args = new XAddArgs()
                .maxlen(streamMaxLen)
                .nearlyExactTrimming();
        Map<String, String> fields = fields(plan, document, crawlId, request);
        String redisId = streams.xadd(streamKey, args, fields);
        LOG.infof("XADD opensearch-indexing: stream=%s redisId=%s doc=%s plan=%s index=%s",
                streamKey, redisId, document.getDocId(), plan.id(), plan.indexName());
        return redisId;
    }

    /**
     * Pure-CPU build of the protobuf request the consumer will eventually act
     * on. Stamps {@code crawl_id} onto the base document and (for chunked
     * strategies) onto every chunk so per-run progress queries can filter.
     * Mirrors what {@link SchemaManagerService#indexDocumentViaManager} used to
     * build inline; extracted here so the indexing-via-redis path doesn't
     * depend on the manager-bidi-stream code at all.
     */
    @VisibleForTesting
    StreamIndexDocumentsRequest buildIndexRequest(ResolvedPlan plan, PipeDoc document, OpenSearchDocument osDoc, String crawlId) {
        String indexName = plan.indexName();
        OpenSearchDocument stampedOsDoc = (crawlId == null || crawlId.isEmpty())
                ? osDoc
                : osDoc.toBuilder().setCrawlId(crawlId).build();

        StreamIndexDocumentsRequest.Builder builder = StreamIndexDocumentsRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
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

            LOG.debugf("Built indexing request with %d chunks for strategy %s (doc %s, plan %s)",
                    chunkResult.chunkDocuments().size(), protoStrategy, document.getDocId(), plan.id());
        }

        return builder.build();
    }

    private Map<String, String> fields(ResolvedPlan plan, PipeDoc document, String crawlId, StreamIndexDocumentsRequest request) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(FIELD_JOB_ID, UUID.randomUUID().toString());
        map.put(FIELD_DOC_ID, document.getDocId());
        map.put(FIELD_PLAN_ID, plan.id());
        map.put(FIELD_INDEX_NAME, plan.indexName());
        map.put(FIELD_ACCOUNT_ID, document.hasOwnership() ? document.getOwnership().getAccountId() : "");
        map.put(FIELD_CRAWL_ID, crawlId == null ? "" : crawlId);
        map.put(FIELD_REQUEST_PAYLOAD, Base64.getEncoder().encodeToString(request.toByteArray()));
        return map;
    }

    /**
     * Decode the {@link #FIELD_REQUEST_PAYLOAD} field of an XADD entry back
     * into a {@link StreamIndexDocumentsRequest}. Lives here so the Phase 2
     * consumer (in opensearch-manager) can call the same decoder without
     * pulling in a sink dependency — same producer/consumer contract.
     */
    public static StreamIndexDocumentsRequest decodeRequest(String base64Payload) {
        try {
            return StreamIndexDocumentsRequest.parseFrom(Base64.getDecoder().decode(base64Payload));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Malformed opensearch-indexing request payload", e);
        }
    }
}
