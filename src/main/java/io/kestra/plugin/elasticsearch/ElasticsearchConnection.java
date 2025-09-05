package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.ssl.SSLContexts;

import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.net.ssl.SSLContext;

@SuperBuilder
@NoArgsConstructor
@Getter
public class ElasticsearchConnection {

    @Schema(
        title = "List of HTTP ElasticSearch servers.",
        description = "Must be an URI like `https://elasticsearch.com:9200` with scheme and port."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<String> hosts;

    @Schema(
        title = "Basic auth configuration."
    )
    @PluginProperty
    private BasicAuth basicAuth;

    @Schema(
        title = "List of HTTP headers to be send on every request.",
        description = "Must be a string with key value separated with `:`, ex: `Authorization: Token XYZ`."
    )
    private Property<List<String>> headers;

    @Schema(
        title = "Sets the path's prefix for every request used by the HTTP client.",
        description = "For example, if this is set to `/my/path`, then any client request will become `/my/path/` + endpoint.\n" +
            "In essence, every request's endpoint is prefixed by this `pathPrefix`.\n" +
            "The path prefix is useful for when ElasticSearch is behind a proxy that provides a base path " +
            "or a proxy that requires all paths to start with '/'; it is not intended for other purposes and " +
            "it should not be supplied in other scenarios."
    )
    private Property<String> pathPrefix;

    @Schema(
        title = "Whether the REST client should return any response containing at least one warning header as a failure."
    )
    private Property<Boolean> strictDeprecationMode;

    @Schema(
        title = "Trust all SSL CA certificates.",
        description = "Use this if the server is using a self signed SSL certificate."
    )
    private Property<Boolean> trustAllSsl;

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class BasicAuth {
        @Schema(
            title = "Basic auth username."
        )
        private Property<String> username;

        @Schema(
            title = "Basic auth password."
        )
        private Property<String> password;
    }

    Rest5Client client(RunContext runContext) throws IllegalVariableEvaluationException {
        PoolingAsyncClientConnectionManagerBuilder connectionManagerBuilder = PoolingAsyncClientConnectionManagerBuilder.create();
        if (runContext.render(this.trustAllSsl).as(Boolean.class).orElse(false)) {
            try {
                SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
                    .build();

                connectionManagerBuilder.setTlsStrategy(new DefaultClientTlsStrategy(sslContext, NoopHostnameVerifier.INSTANCE));
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
                throw new IllegalArgumentException(e);
            }
        }

        HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create()
            .setConnectionManager(connectionManagerBuilder.build());

        httpClientBuilder.setUserAgent("Kestra/" + runContext.version());

        Rest5ClientBuilder builder = Rest5Client
            .builder(this.httpHosts(runContext))
            .setHttpClientConfigCallback(client -> httpClientBuilder.build());


        if (this.getHeaders() != null) {
            builder.setDefaultHeaders(this.defaultHeaders(runContext));
        }

        if (runContext.render(this.pathPrefix).as(String.class).isPresent()) {
            builder.setPathPrefix(runContext.render(this.pathPrefix).as(String.class).get());
        }

        if (runContext.render(this.strictDeprecationMode).as(Boolean.class).isPresent()) {
            builder.setStrictDeprecationMode(runContext.render(this.strictDeprecationMode).as(Boolean.class).get());
        }


        return builder.build();
    }

    public ElasticsearchClient highLevelClient(RunContext runContext) throws IllegalVariableEvaluationException {
        Rest5Client lowLevelClient = client(runContext);
        Rest5ClientTransport transport = new Rest5ClientTransport(lowLevelClient, new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

    private HttpHost[] httpHosts(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.hosts)
            .stream()
            .map(s -> {
                URI uri = URI.create(s);
                return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
            })
            .toArray(HttpHost[]::new);
    }

    private Header[] defaultHeaders(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.headers).asList(String.class)
            .stream()
            .map(header -> {
                String[] nameAndValue = header.split(":");
                return new BasicHeader(nameAndValue[0], nameAndValue[1]);
            })
            .toArray(Header[]::new);
    }
}
