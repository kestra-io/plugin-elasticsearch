package io.kestra.plugin.elasticsearch;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.elasticsearch.shared.ElasticsearchConnection;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;

@KestraTest
class SearchRegressionTest {
    private static HttpServer server;
    private static String host;
    private static boolean includeTotal;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange ->
        {
            String total = includeTotal ? ",\"total\":{\"value\":1,\"relation\":\"eq\"}" : "";
            String response = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0}," +
                "\"hits\":{\"max_score\":1.0,\"hits\":[{\"_index\":\"search-regression\",\"_id\":\"1\",\"_score\":1.0,\"_source\":{\"status\":\"present\"}}]" + total + "}}";
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
            exchange.sendResponseHeaders(200, payload.length);
            try (var output = exchange.getResponseBody()) {
                output.write(payload);
            }
        });
        server.start();
        host = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void succeedsWhenElasticsearchOmitsTotalHits() throws Exception {
        includeTotal = false;

        Search.Output output = task("{\"query\":{\"match_all\":{}},\"track_total_hits\":false}").run(runContextFactory.of());

        assertThat(output.getSize(), is(1));
        assertThat(output.getRows().getFirst().get("status"), is("present"));
        assertNull(output.getTotal());
    }

    @Test
    void preservesReportedTotalHits() throws Exception {
        includeTotal = true;

        Search.Output output = task("{\"query\":{\"match_all\":{}}}").run(runContextFactory.of());

        assertThat(output.getSize(), is(1));
        assertThat(output.getTotal(), is(1L));
    }

    private Search task(String request) {
        return Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(List.of(host)).build())
            .indexes(Property.ofValue(List.of("search-regression")))
            .request(request)
            .build();
    }
}
