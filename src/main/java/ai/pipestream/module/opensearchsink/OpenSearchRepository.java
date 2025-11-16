package ai.pipestream.module.opensearchsink;

import ai.pipestream.module.opensearchsink.opensearch.ReactiveOpenSearchClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

import java.util.List;

/**
 * A reactive repository for interacting with OpenSearch.
 * This class is a thin wrapper around the OpenSearchAsyncClient to isolate data access logic.
 */
@ApplicationScoped
public class OpenSearchRepository {

    private final ReactiveOpenSearchClient reactiveClient;

    @Inject
    public OpenSearchRepository(ReactiveOpenSearchClient reactiveClient) {
        this.reactiveClient = reactiveClient;
    }

    /**
     * Performs a bulk indexing or deleting operation asynchronously.
     *
     * @param operations The list of bulk operations to perform.
     * @return a Uni that will resolve to the BulkResponse.
     */
    public Uni<BulkResponse> bulk(List<BulkOperation> operations) {
        if (operations == null || operations.isEmpty()) {
            return Uni.createFrom().item(BulkResponse.of(b -> b.items(List.of()).errors(false).took(0)));
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        return reactiveClient.bulk(bulkRequest);
    }
}