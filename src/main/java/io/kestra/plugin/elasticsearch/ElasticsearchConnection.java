package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientOptions;
import co.elastic.clients.transport.rest5_client.low_level.RequestOptions;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
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

@SuperBuilder
@NoArgsConstructor
@Getter
public class ElasticsearchConnection {
    private static final int DEFAULT_TARGET_SERVER_VERSION = 8;
    private static final int MAX_SUPPORTED_TARGET_SERVER_VERSION = 9;
    private static final String ACCEPT_HEADER = "Accept";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String COMPATIBLE_MEDIA_TYPE = "application/vnd.elasticsearch+json; compatible-with=%d";

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

    @Schema(
        title = "Target Elasticsearch server major version",
        description = "Major version used for compatibility headers (`Accept` and `Content-Type`). Set to `8` for Elasticsearch 8 clusters or `9` for Elasticsearch 9 clusters."
    )
    @Builder.Default
    private Property<Integer> targetServerVersion = Property.ofValue(DEFAULT_TARGET_SERVER_VERSION);

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

    Rest5Client client(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = Rest5Client.builder(this.httpHosts(runContext));
        var defaultHeaders = this.headers != null ? this.defaultHeaders(runContext) : null;
        var credentialsProvider = this.credentialsProvider(runContext, defaultHeaders);

        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setUserAgent("Kestra/" + runContext.version());
            if (credentialsProvider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        if (runContext.render(this.trustAllSsl).as(Boolean.class).orElse(false)) {
            try {
                var sslContext = SSLContexts.custom()
                    .loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
                    .build();

                builder.setConnectionManagerCallback(connectionManagerBuilder ->
                    connectionManagerBuilder.setTlsStrategy(new DefaultClientTlsStrategy(sslContext, NoopHostnameVerifier.INSTANCE))
                );
            } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (defaultHeaders != null) {
            builder.setDefaultHeaders(defaultHeaders);
        }

        if (runContext.render(this.pathPrefix).as(String.class).isPresent()) {
            builder.setPathPrefix(runContext.render(this.pathPrefix).as(String.class).get());
        }

        if (runContext.render(this.strictDeprecationMode).as(Boolean.class).isPresent()) {
            builder.setStrictDeprecationMode(runContext.render(this.strictDeprecationMode).as(Boolean.class).get());
        }


        return builder.build();
    }

    private BasicCredentialsProvider credentialsProvider(RunContext runContext, Header[] defaultHeaders) throws IllegalVariableEvaluationException {
        if (this.basicAuth == null) {
            return null;
        }
        if (this.hasAuthorizationHeader(defaultHeaders)) {
            return null;
        }

        var username = runContext.render(this.basicAuth.username).as(String.class).orElseThrow();
        var password = runContext.render(this.basicAuth.password).as(String.class).orElseThrow();
        var credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(
            new AuthScope(null, null, -1, null, null),
            new UsernamePasswordCredentials(username, password.toCharArray())
        );
        return credentialProvider;
    }

    private boolean hasAuthorizationHeader(Header[] defaultHeaders) {
        if (defaultHeaders == null) {
            return false;
        }

        for (var header : defaultHeaders) {
            if ("Authorization".equalsIgnoreCase(header.getName())) {
                return true;
            }
        }
        return false;
    }

    public ElasticsearchClient highLevelClient(RunContext runContext) throws IllegalVariableEvaluationException {
        var lowLevelClient = client(runContext);
        var compatibleMediaType = this.compatibleMediaType(runContext);
        var transportOptionsBuilder = new Rest5ClientOptions.Builder(RequestOptions.DEFAULT.toBuilder());
        transportOptionsBuilder.setHeader(ACCEPT_HEADER, compatibleMediaType);
        transportOptionsBuilder.setHeader(CONTENT_TYPE_HEADER, compatibleMediaType);
        var transportOptions = transportOptionsBuilder.build();
        var transport = new Rest5ClientTransport(lowLevelClient, new JacksonJsonpMapper(), transportOptions);

        return new ElasticsearchClient(transport);
    }

    private HttpHost[] httpHosts(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.hosts)
            .stream()
            .map(s -> {
                var uri = URI.create(s);
                return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
            })
            .toArray(HttpHost[]::new);
    }

    private Header[] defaultHeaders(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.headers).asList(String.class)
            .stream()
            .map(header -> {
                var nameAndValue = header.split(":", 2);
                if (nameAndValue.length != 2) {
                    throw new IllegalArgumentException("Invalid header format, expected `Name: Value` but got `" + header + "`");
                }
                return new BasicHeader(nameAndValue[0].trim(), nameAndValue[1].trim());
            })
            .toArray(Header[]::new);
    }

    String compatibleMediaType(RunContext runContext) throws IllegalVariableEvaluationException {
        var rTargetServerVersion = runContext.render(this.targetServerVersion).as(Integer.class).orElse(DEFAULT_TARGET_SERVER_VERSION);
        if (rTargetServerVersion < DEFAULT_TARGET_SERVER_VERSION || rTargetServerVersion > MAX_SUPPORTED_TARGET_SERVER_VERSION) {
            throw new IllegalArgumentException("`targetServerVersion` must be 8 or 9");
        }

        return COMPATIBLE_MEDIA_TYPE.formatted(rTargetServerVersion);
    }
}
