package ai.pipestream.module.opensearchsink.opensearch;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsRequest;

import java.io.IOException;

/**
 * A reactive wrapper around the synchronous OpenSearchClient.
 * This allows us to perform non-blocking operations by running the blocking
 * calls on a separate worker thread pool.
 */
@ApplicationScoped
public class ReactiveOpenSearchClient {

    private final OpenSearchClient client;

    @Inject
    public ReactiveOpenSearchClient(OpenSearchClient client) {
        this.client = client;
    }

    public Uni<Boolean> indexExists(String indexName) {
        return Uni.createFrom().item(() -> {
            try {
                return client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        return Uni.createFrom().item(() -> {
            try {
                return client.indices().create(request);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<BulkResponse> bulk(BulkRequest request) {
        return Uni.createFrom().item(() -> {
            try {
                return client.bulk(request);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
