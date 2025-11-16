package ai.pipestream.module.opensearchsink.util;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;

import javax.net.ssl.SSLContext;
import java.util.Optional;

@ApplicationScoped
public class OpenSearchClientProducer {

    @ConfigProperty(name = "opensearch.hosts", defaultValue = "localhost:9200")
    String hosts;

    @ConfigProperty(name = "opensearch.protocol", defaultValue = "http")
    String protocol;

    @ConfigProperty(name = "opensearch.username")
    Optional<String> username;

    @ConfigProperty(name = "opensearch.password")
    Optional<String> password;

    @ConfigProperty(name = "opensearch.ssl.verify", defaultValue = "true")
    boolean sslVerify;

    @ConfigProperty(name = "opensearch.connection-timeout", defaultValue = "5000")
    int connectTimeout;

    @ConfigProperty(name = "opensearch.socket-timeout", defaultValue = "10000")
    int socketTimeout;

    @Produces
    @Singleton
    public ApacheHttpClient5Transport openSearchTransport() {
        try {
            String[] hostParts = hosts.split(",");
            HttpHost[] httpHosts = new HttpHost[hostParts.length];
            for (int i = 0; i < hostParts.length; i++) {
                httpHosts[i] = HttpHost.create(hostParts[i]);
            }

            SSLContext sslContext = "https".equals(protocol) && !sslVerify
                    ? SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build()
                    : null;

            var transportBuilder = ApacheHttpClient5TransportBuilder.builder(httpHosts);
            transportBuilder.setMapper(new JacksonJsonpMapper());
            transportBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                if (username.isPresent() && password.isPresent() && 
                    !username.get().isBlank() && !password.get().isBlank()) {
                    final var credentialsProvider = new BasicCredentialsProvider();
                    for (final var httpHost : httpHosts) {
                        credentialsProvider.setCredentials(new AuthScope(httpHost), 
                            new UsernamePasswordCredentials(username.get(), password.get().toCharArray()));
                    }
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
                if (sslContext != null) {
                    final var tlsStrategy = ClientTlsStrategyBuilder.create().setSslContext(sslContext).setHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
                    final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();
                    httpClientBuilder.setConnectionManager(connectionManager);
                }

                // Set timeouts from application.properties
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                        .setResponseTimeout(Timeout.ofMilliseconds(socketTimeout))
                        .build();
                httpClientBuilder.setDefaultRequestConfig(requestConfig);

                return httpClientBuilder;
            });

            return transportBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create OpenSearch transport", e);
        }
    }

    @Produces
    @Singleton
    public OpenSearchClient openSearchClient(ApacheHttpClient5Transport transport) {
        return new OpenSearchClient(transport);
    }

    @Produces
    @Singleton
    public OpenSearchAsyncClient openSearchAsyncClient(ApacheHttpClient5Transport transport) {
        return new OpenSearchAsyncClient(transport);
    }
}