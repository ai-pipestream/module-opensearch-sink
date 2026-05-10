package ai.pipestream.module.opensearchsink;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchChunkDocument;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchDocumentMap;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.stream.XAddArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the on-the-wire shape of the sink's XADD: which stream gets the
 * entry, which fields are populated, and that the protobuf payload round-trips
 * cleanly back through the static decoder.
 */
class OpenSearchIndexingPublisherTest {

    @SuppressWarnings("unchecked")
    private final StreamCommands<String, String, String> streams =
            (StreamCommands<String, String, String>) mock(StreamCommands.class);
    private final RedisDataSource redis = mock(RedisDataSource.class);
    private final ChunkDocumentConverter chunkConverter = mock(ChunkDocumentConverter.class);
    private final OpenSearchIndexingPublisher publisher = new OpenSearchIndexingPublisher();

    @BeforeEach
    void setUp() {
        publisher.redis = redis;
        publisher.chunkDocumentConverter = chunkConverter;
        publisher.streamMaxLen = 1_000_000L;
        when(redis.stream(String.class)).thenReturn(streams);
        publisher.init();
        when(streams.xadd(anyString(), any(XAddArgs.class), any(Map.class)))
                .thenReturn("1700000000000-0");
    }

    @Test
    void publishWritesToPerPlanStreamKey() {
        ResolvedPlan plan = new ResolvedPlan(
                "plan-nested-7", "docs-nested-7",
                IndexingStrategy.INDEXING_STRATEGY_NESTED, List.of());
        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-1").build();
        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder().setOriginalDocId("doc-1").build();

        String redisId = publisher.publish(plan, doc, osDoc, "crawl-A");

        assertThat(redisId)
                .as("publish returns the redis-broker assigned entry id")
                .isEqualTo("1700000000000-0");

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(streams).xadd(keyCaptor.capture(), any(XAddArgs.class), fieldsCaptor.capture());
        assertThat(keyCaptor.getValue())
                .as("stream key is pipestream:indexing:<plan_id>")
                .isEqualTo("pipestream:indexing:plan-nested-7");

        Map<String, String> fields = fieldsCaptor.getValue();
        assertThat(fields)
                .as("XADD entry has all the fields a debug tool / consumer needs without parsing protobuf")
                .containsKeys(
                        OpenSearchIndexingPublisher.FIELD_JOB_ID,
                        OpenSearchIndexingPublisher.FIELD_DOC_ID,
                        OpenSearchIndexingPublisher.FIELD_PLAN_ID,
                        OpenSearchIndexingPublisher.FIELD_INDEX_NAME,
                        OpenSearchIndexingPublisher.FIELD_ACCOUNT_ID,
                        OpenSearchIndexingPublisher.FIELD_CRAWL_ID,
                        OpenSearchIndexingPublisher.FIELD_REQUEST_PAYLOAD);
        assertThat(fields.get(OpenSearchIndexingPublisher.FIELD_DOC_ID)).isEqualTo("doc-1");
        assertThat(fields.get(OpenSearchIndexingPublisher.FIELD_PLAN_ID)).isEqualTo("plan-nested-7");
        assertThat(fields.get(OpenSearchIndexingPublisher.FIELD_INDEX_NAME)).isEqualTo("docs-nested-7");
        assertThat(fields.get(OpenSearchIndexingPublisher.FIELD_CRAWL_ID)).isEqualTo("crawl-A");
        assertThat(fields.get(OpenSearchIndexingPublisher.FIELD_ACCOUNT_ID))
                .as("doc without ownership emits an empty account_id field rather than dropping it")
                .isEmpty();
    }

    @Test
    void payloadRoundTripsThroughDecoder() {
        ResolvedPlan plan = new ResolvedPlan(
                "plan-round-trip", "rt-index",
                IndexingStrategy.INDEXING_STRATEGY_NESTED, List.of());
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-rt")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("acct-42")
                        .setDatasourceId("ds-99")
                        .build())
                .build();
        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder().setOriginalDocId("doc-rt").build();

        publisher.publish(plan, doc, osDoc, "");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(streams).xadd(anyString(), any(XAddArgs.class), fieldsCaptor.capture());
        String payload = fieldsCaptor.getValue().get(OpenSearchIndexingPublisher.FIELD_REQUEST_PAYLOAD);

        StreamIndexDocumentsRequest decoded = OpenSearchIndexingPublisher.decodeRequest(payload);
        assertThat(decoded.getDocumentId()).isEqualTo("doc-rt");
        assertThat(decoded.getIndexName()).isEqualTo("rt-index");
        assertThat(decoded.getAccountId()).isEqualTo("acct-42");
        assertThat(decoded.getDatasourceId()).isEqualTo("ds-99");
        assertThat(decoded.getIndexingStrategy()).isEqualTo(IndexingStrategy.INDEXING_STRATEGY_NESTED);
    }

    @Test
    void chunkedStrategyStampsCrawlIdOnEveryChunk() {
        ResolvedPlan plan = new ResolvedPlan(
                "plan-chunk", "chunked-index",
                IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED, List.of());
        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-chunk").build();
        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder().setOriginalDocId("doc-chunk").build();

        OpenSearchChunkDocument unstampedChunkA = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-chunk").setChunkIndex(0).build();
        OpenSearchChunkDocument unstampedChunkB = OpenSearchChunkDocument.newBuilder()
                .setDocId("doc-chunk").setChunkIndex(1).build();
        when(chunkConverter.convertToChunks(eq(doc), any(OpenSearchDocument.class),
                eq("chunked-index"), eq(IndexingStrategy.INDEXING_STRATEGY_CHUNK_COMBINED)))
                .thenReturn(new ChunkConversionResult(
                        OpenSearchDocumentMap.newBuilder().build(),
                        List.of(unstampedChunkA, unstampedChunkB),
                        List.of()));

        StreamIndexDocumentsRequest built = publisher.buildIndexRequest(plan, doc, osDoc, "crawl-X");

        assertThat(built.getChunkDocumentsCount())
                .as("every chunk produced by the converter ends up on the request")
                .isEqualTo(2);
        assertThat(built.getChunkDocumentsList())
                .extracting(OpenSearchChunkDocument::getCrawlId)
                .as("crawl_id is stamped onto every chunk so per-run progress queries find them")
                .containsExactly("crawl-X", "crawl-X");
        assertThat(built.getDocument().getCrawlId())
                .as("base document also carries the crawl_id stamp")
                .isEqualTo("crawl-X");
    }

    @Test
    void unspecifiedStrategyRefusesLoudInsteadOfMisrouting() {
        ResolvedPlan badPlan = new ResolvedPlan(
                "plan-broken", "anywhere",
                IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED, List.of());
        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-x").build();

        assertThatThrownBy(() -> publisher.publish(
                badPlan, doc, OpenSearchDocument.getDefaultInstance(), ""))
                .as("a READY plan with UNSPECIFIED strategy is a manager contract violation; "
                        + "publish must refuse, not silently pick a default")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UNSPECIFIED");
    }
}
