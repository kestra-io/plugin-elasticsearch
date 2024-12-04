package io.kestra.plugin.elasticsearch;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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

import java.net.URI;
import java.util.List;
import javax.net.ssl.SSLContext;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
public class ElasticsearchConnection {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson(false);

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

    RestClientTransport client(RunContext runContext) throws IllegalVariableEvaluationException {
        RestClientBuilder builder = RestClient
            .builder(this.httpHosts(runContext))
            .setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder = this.httpAsyncClientBuilder(runContext);
                return httpClientBuilder;
            });

        if (this.getHeaders() != null) {
            builder.setDefaultHeaders(this.defaultHeaders(runContext));
        }

        if (runContext.render(this.pathPrefix).as(String.class).isPresent()) {
            builder.setPathPrefix(runContext.render(this.pathPrefix).as(String.class).get());
        }

        if (runContext.render(this.strictDeprecationMode).as(Boolean.class).isPresent()) {
            builder.setStrictDeprecationMode(runContext.render(this.strictDeprecationMode).as(Boolean.class).get());
        }

        return new RestClientTransport(builder.build(), new JacksonJsonpMapper(MAPPER));
    }

    @SneakyThrows
    private HttpAsyncClientBuilder httpAsyncClientBuilder(RunContext runContext) {
        HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create();

        builder.setUserAgent("Kestra/" + runContext.version());

        if (basicAuth != null) {
            final CredentialsProvider basicCredential = new BasicCredentialsProvider();
            basicCredential.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(
                    runContext.render(this.basicAuth.username).as(String.class).orElseThrow(),
                    runContext.render(this.basicAuth.password).as(String.class).orElseThrow()
                )
            );

            builder.setDefaultCredentialsProvider(basicCredential);
        }

        if (runContext.render(this.trustAllSsl).as(Boolean.class).orElse(false)) {
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
        return runContext.render(this.headers).asList(String.class)
            .stream()
            .map(header -> {
                String[] nameAndValue = header.split(":");
                return new BasicHeader(nameAndValue[0], nameAndValue[1]);
            })
            .toArray(Header[]::new);
    }
}
