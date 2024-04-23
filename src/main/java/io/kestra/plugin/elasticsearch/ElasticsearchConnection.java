package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.VersionProvider;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.net.URI;
import java.util.List;
import javax.net.ssl.SSLContext;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

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
    @PluginProperty(dynamic = false)
    private BasicAuth basicAuth;

    @Schema(
        title = "List of HTTP headers to be send on every request.",
        description = "Must be a string with key value separated with `:`, ex: `Authorization: Token XYZ`."
    )
    @PluginProperty(dynamic = true)
    private List<String> headers;

    @Schema(
        title = "Sets the path's prefix for every request used by the HTTP client.",
        description = "For example, if this is set to `/my/path`, then any client request will become `/my/path/` + endpoint.\n" +
            "In essence, every request's endpoint is prefixed by this `pathPrefix`.\n" +
            "The path prefix is useful for when ElasticSearch is behind a proxy that provides a base path " +
            "or a proxy that requires all paths to start with '/'; it is not intended for other purposes and " +
            "it should not be supplied in other scenarios."
    )
    @PluginProperty(dynamic = true)
    private String pathPrefix;

    @Schema(
        title = "Whether the REST client should return any response containing at leas one warning header as a failure."
    )
    @PluginProperty(dynamic = false)
    private Boolean strictDeprecationMode;

    @Schema(
        title = "Trust all SSL CA certificates.",
        description = "Use this if the server is using a self signed SSL certificate."
    )
    @PluginProperty(dynamic = false)
    private Boolean trustAllSsl;

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class BasicAuth {
        @Schema(
            title = "Basic auth username."
        )
        @PluginProperty(dynamic = true)
        private String username;

        @Schema(
            title = "Basic auth password."
        )
        @PluginProperty(dynamic = true)
        private String password;
    }

    RestHighLevelClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        RestClientBuilder builder = RestClient
            .builder(this.httpHosts(runContext))
            .setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder = this.httpAsyncClientBuilder(runContext);
                return httpClientBuilder;
            });

        if (this.getHeaders() != null) {
            builder.setDefaultHeaders(this.defaultHeaders(runContext));
        }

        if (this.getPathPrefix() != null) {
            builder.setPathPrefix(runContext.render(this.pathPrefix));
        }

        if (this.getStrictDeprecationMode() != null) {
            builder.setStrictDeprecationMode(this.getStrictDeprecationMode());
        }

        return new RestHighLevelClient(builder);
    }

    @SneakyThrows
    private HttpAsyncClientBuilder httpAsyncClientBuilder(RunContext runContext) {
        HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create();

        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);
        builder.setUserAgent("Kestra/" + versionProvider.getVersion());

        if (basicAuth != null) {
            final CredentialsProvider basicCredential = new BasicCredentialsProvider();
            basicCredential.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(
                    runContext.render(this.basicAuth.username),
                    runContext.render(this.basicAuth.password)
                )
            );

            builder.setDefaultCredentialsProvider(basicCredential);
        }

        if (trustAllSsl != null && trustAllSsl) {
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            sslContextBuilder.loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true);
            SSLContext sslContext = sslContextBuilder.build();

            builder.setSSLContext(sslContext);
            builder.setSSLHostnameVerifier(new NoopHostnameVerifier());
        }

        return builder;
    }

    private HttpHost[] httpHosts(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.hosts)
            .stream()
            .map(s -> {
                URI uri = URI.create(s);
                return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
            })
            .toArray(HttpHost[]::new);
    }

    private Header[] defaultHeaders(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.headers)
            .stream()
            .map(header -> {
                String[] nameAndValue = header.split(":");
                return new BasicHeader(nameAndValue[0], nameAndValue[1]);
            })
            .toArray(Header[]::new);
    }
}
