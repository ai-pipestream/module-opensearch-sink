package ai.pipestream.module.opensearchsink;

import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.module.opensearchsink.plan.ResolvedPlan;
import ai.pipestream.module.opensearchsink.service.ChunkConversionResult;
import ai.pipestream.module.opensearchsink.service.ChunkDocumentConverter;
import ai.pipestream.module.opensearchsink.service.RepoClient;
import ai.pipestream.opensearch.v1.IndexingStrategy;
import ai.pipestream.opensearch.v1.OpenSearchChunkDocument;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchDocumentMap;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocResponse;
import ai.pipestream.repository.v1.IndexingRequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OpenSearchIndexingPublisherTest {

    private final RepoClient repoClient = mock(RepoClient.class);
    @SuppressWarnings("unchecked")
    private final ProtobufEmitter<IndexingRequestEvent> emitter = mock(ProtobufEmitter.class);
    private final ChunkDocumentConverter chunkConverter = mock(ChunkDocumentConverter.class);
    private final OpenSearchIndexingPublisher publisher = new OpenSearchIndexingPublisher();

    @BeforeEach
    void setUp() {
        publisher.repoClient = repoClient;
        publisher.emitter = emitter;
        publisher.chunkDocumentConverter = chunkConverter;

        when(repoClient.savePipeDoc(any(SavePipeDocRequest.class)))
                .thenReturn(SavePipeDocResponse.newBuilder()
                        .setNodeId("saved-node-id")
                        .build());
        when(emitter.send(any(IndexingRequestEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void publishSavesToRepoAndEmitsKafkaEvent() throws Exception {
        ResolvedPlan plan = new ResolvedPlan(
                "plan-nested-7", "docs-nested-7",
                IndexingStrategy.INDEXING_STRATEGY_NESTED, List.of());
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-1")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("acct-42")
                        .setDatasourceId("ds-99")
                        .build())
                .build();
        OpenSearchDocument osDoc = OpenSearchDocument.newBuilder().setOriginalDocId("doc-1").build();

        String nodeId = publisher.publish(plan, doc, osDoc, "crawl-A");

        assertThat(nodeId).isEqualTo("saved-node-id");

        // Verify save request
        ArgumentCaptor<SavePipeDocRequest> saveCaptor = ArgumentCaptor.forClass(SavePipeDocRequest.class);
        verify(repoClient).savePipeDoc(saveCaptor.capture());
        SavePipeDocRequest saveReq = saveCaptor.getValue();
        assertThat(saveReq.getDrive()).isEqualTo("acct-42:index:docs-nested-7");
        assertThat(saveReq.getConnectorId()).isEqualTo("ds-99");
        assertThat(saveReq.getUseDatasourceId()).isTrue();

        // Verify Kafka event
        ArgumentCaptor<IndexingRequestEvent> eventCaptor = ArgumentCaptor.forClass(IndexingRequestEvent.class);
        verify(emitter).send(eventCaptor.capture());
        IndexingRequestEvent event = eventCaptor.getValue();
        assertThat(event.getEventId()).isNotEmpty();
        assertThat(event.getDocumentRef().getDocId()).isEqualTo("doc-1");
        assertThat(event.getDocumentRef().getAccountId()).isEqualTo("acct-42");
        assertThat(event.getDocumentRef().getGraphAddressId()).isEqualTo("ds-99");
        assertThat(event.getPlanId()).isEqualTo("plan-nested-7");
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

        assertThat(built.getChunkDocumentsCount()).isEqualTo(2);
        assertThat(built.getChunkDocumentsList())
                .extracting(OpenSearchChunkDocument::getCrawlId)
                .containsExactly("crawl-X", "crawl-X");
        assertThat(built.getDocument().getCrawlId()).isEqualTo("crawl-X");
    }

    @Test
    void unspecifiedStrategyRefusesLoudInsteadOfMisrouting() {
        ResolvedPlan badPlan = new ResolvedPlan(
                "plan-broken", "anywhere",
                IndexingStrategy.INDEXING_STRATEGY_UNSPECIFIED, List.of());
        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-x").build();

        assertThatThrownBy(() -> publisher.publish(
                badPlan, doc, OpenSearchDocument.getDefaultInstance(), ""))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UNSPECIFIED");
    }
}
