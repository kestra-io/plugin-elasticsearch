package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.sun.net.httpserver.HttpServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@KestraTest
class BasicAuthRegressionTest {
    private static final String USERNAME = "elastic";
    private static final String PASSWORD = "changeme";
    private static final String EXPECTED_AUTHORIZATION = "Basic " + Base64.getEncoder()
        .encodeToString((USERNAME + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));

    private static HttpServer server;
    private static String host;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void beforeAll() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            var authorization = exchange.getRequestHeaders().getFirst("Authorization");

            if (!EXPECTED_AUTHORIZATION.equals(authorization)) {
                var body = "unauthorized";
                var payload = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("content-type", "text/plain");
                exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"test\"");
                exchange.sendResponseHeaders(401, payload.length);
                try (var output = exchange.getResponseBody()) {
                    output.write(payload);
                }
                return;
            }

            var body = """
                {"_index":"auth_regression","_id":"doc-1","_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
                """;
            var payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(201, payload.length);
            try (var output = exchange.getResponseBody()) {
                output.write(payload);
            }
        });
        server.start();
        host = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void afterAll() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldSendBasicAuthForPut() throws Exception {
        var runContext = runContextFactory.of();
        var task = Put.builder()
            .connection(
                ElasticsearchConnection.builder()
                    .hosts(List.of(host))
                    .basicAuth(ElasticsearchConnection.BasicAuth.builder()
                        .username(Property.ofValue(USERNAME))
                        .password(Property.ofValue(PASSWORD))
                        .build())
                    .build()
            )
            .index(Property.ofValue("auth_regression"))
            .key(Property.ofValue("doc-1"))
            .value(Map.of("name", "john"))
            .build();

        var output = task.run(runContext);

        assertThat(output.getId(), is("doc-1"));
    }
}
