package ai.pipestream.module.opensearchsink.util;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.CountRequest;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.RefreshRequest;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.IOException;

/**
 * A test helper client to interact with the OpenSearch container directly.
 * This uses the modern opensearch-java client and transport.
 */
public class OpenSearchTestClient {

    private final OpenSearchClient client;

    public OpenSearchTestClient(String host, int port) {
        HttpHost httpHost = new HttpHost("http", host, port);
        ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder.builder(httpHost)
                .setMapper(new JacksonJsonpMapper())
                .build();
        this.client = new OpenSearchClient(transport);
    }

    public void deleteIndex(String indexName) throws IOException {
        if (client.indices().exists(r -> r.index(indexName)).value()) {
            client.indices().delete(d -> d.index(indexName));
        }
    }

    public void refreshIndex(String indexName) throws IOException {
        client.indices().refresh(r -> r.index(indexName));
    }

    public long countDocuments(String indexName) throws IOException {
        if (!client.indices().exists(r -> r.index(indexName)).value()) {
            return 0;
        }
        CountResponse response = client.count(c -> c.index(indexName));
        return response.count();
    }

    public void close() throws IOException {
        this.client._transport().close();
    }
}