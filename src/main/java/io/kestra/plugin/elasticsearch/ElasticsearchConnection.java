package io.kestra.plugin.elasticsearch;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.instrumentation.NoopInstrumentation;
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
        title = "Elasticsearch hosts",
        description = "List of HTTP(S) endpoints including scheme and port, e.g. `https://elasticsearch.com:9200`; at least one is required."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<String> hosts;

    @Schema(
        title = "Basic authentication",
        description = "Optional HTTP basic auth credentials rendered at runtime."
    )
    @PluginProperty
    private BasicAuth basicAuth;

    @Schema(
        title = "Custom HTTP headers",
        description = "Headers sent on every request in `Name: Value` format, e.g. `Authorization: Token XYZ`."
    )
    private Property<List<String>> headers;

    @Schema(
        title = "Request path prefix",
        description = "Base path prepended to every Elasticsearch endpoint, e.g. `/my/path`. Use only when the cluster is served behind a proxy that requires a prefix; leave empty otherwise."
    )
    private Property<String> pathPrefix;

    @Schema(
        title = "Fail on warning headers",
        description = "When true, any response containing Elasticsearch warning headers is treated as an error."
    )
    private Property<Boolean> strictDeprecationMode;

    @Schema(
        title = "Trust all SSL certificates",
        description = "Skips certificate validation for HTTPS connections; use only with self-signed certificates in non-production."
    )
    private Property<Boolean> trustAllSsl;

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class BasicAuth {
        @Schema(
            title = "Basic auth username",
            description = "Username for HTTP basic authentication."
        )
        private Property<String> username;

        @Schema(
            title = "Basic auth password",
            description = "Password for HTTP basic authentication."
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

        return new RestClientTransport(builder.build(), new JacksonJsonpMapper(MAPPER), null,
            NoopInstrumentation.INSTANCE);
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
