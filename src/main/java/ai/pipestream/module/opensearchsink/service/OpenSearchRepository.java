package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.quarkus.opensearch.grpc.OpenSearchGrpcClientProducer;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkResponse;

@ApplicationScoped
public class OpenSearchRepository {

    private final OpenSearchGrpcClientProducer grpcClient;

    @Inject
    public OpenSearchRepository(OpenSearchGrpcClientProducer grpcClient) {
        this.grpcClient = grpcClient;
    }

    /**
     * Execute a bulk index request via OpenSearch DocumentService gRPC.
     */
    public Uni<BulkResponse> bulk(BulkRequest request) {
        return grpcClient.bulk(request);
    }
}
