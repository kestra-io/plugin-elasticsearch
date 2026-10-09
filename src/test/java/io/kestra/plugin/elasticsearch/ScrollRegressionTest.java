package io.kestra.plugin.elasticsearch;

import java.io.BufferedInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.elasticsearch.shared.ElasticsearchConnection;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class ScrollRegressionTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private HttpServer server;
    private final List<String> requestedScrollIds = new CopyOnWriteArrayList<>();
    private final List<String> clearedScrollIds = new CopyOnWriteArrayList<>();
    private List<List<String>> pages;
    private boolean failScroll;
    private boolean failClear;
    private int page;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange ->
        {
            String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String response;
            int status = 200;
            if (exchange.getRequestMethod().equals("DELETE")) {
                var ids = JacksonMapper.ofJson().readTree(request).get("scroll_id");
                ids.forEach(id -> clearedScrollIds.add(id.asText()));
                response = "{\"succeeded\":true,\"num_freed\":1}";
                if (failClear) {
                    status = 500;
                    response = "{\"error\":{\"type\":\"exception\",\"reason\":\"clear failed\"},\"status\":500}";
                }
            } else if (exchange.getRequestURI().getPath().equals("/_search/scroll")) {
                requestedScrollIds.add(JacksonMapper.ofJson().readTree(request).get("scroll_id").asText());
                page++;
                response = response(page);
                if (failScroll && page == 2) {
                    status = 500;
                    response = "{\"error\":{\"type\":\"exception\",\"reason\":\"scroll failed\"},\"status\":500}";
                }
            } else {
                response = response(0);
            }
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
            exchange.sendResponseHeaders(status, payload.length);
            try (var output = exchange.getResponseBody()) {
                output.write(payload);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private String response(int index) {
        List<String> names = index < pages.size() ? pages.get(index) : List.of();
        String hits = names.stream()
            .map(name -> "{\"_index\":\"test\",\"_id\":\"" + name + "\",\"_source\":{\"name\":\"" + name + "\"}}")
            .collect(java.util.stream.Collectors.joining(","));
        return "{\"_scroll_id\":\"scroll-" + index + "\",\"took\":1,\"timed_out\":false," +
            "\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},\"hits\":{\"hits\":[" + hits + "]}}";
    }

    private Scroll task() {
        return Scroll.builder()
            .id(UUID.randomUUID().toString())
            .type(Scroll.class.getName())
            .connection(ElasticsearchConnection.builder().hosts(List.of("http://127.0.0.1:" + server.getAddress().getPort())).build())
            .indexes(Property.ofValue(List.of("test")))
            .request("{\"size\":2,\"query\":{\"match_all\":{}}}")
            .build();
    }

    private void assertStoredNames(Scroll.Output output, List<String> expected) throws Exception {
        List<String> actual = new ArrayList<>();
        try (var input = new BufferedInputStream(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()))) {
            FileSerde.read(input, row -> actual.add((String) ((Map<?, ?>) row).get("name")));
        }
        assertEquals((long) expected.size(), output.getSize());
        assertEquals(expected, actual);
    }

    @Test
    void writesAllPagesAndClearsLatestScrollId() throws Exception {
        pages = List.of(List.of("one", "two"), List.of("three", "four"), List.of("five"));
        assertStoredNames(task().run(runContextFactory.of()), List.of("one", "two", "three", "four", "five"));
        assertEquals(List.of("scroll-0", "scroll-1", "scroll-2"), requestedScrollIds);
        assertEquals(List.of("scroll-3"), clearedScrollIds);
    }

    @Test
    void writesSinglePage() throws Exception {
        pages = List.of(List.of("one"));
        assertStoredNames(task().run(runContextFactory.of()), List.of("one"));
        assertEquals(List.of("scroll-1"), clearedScrollIds);
    }

    @Test
    void writesEmptyResults() throws Exception {
        pages = List.of(List.of());
        assertStoredNames(task().run(runContextFactory.of()), List.of());
        assertEquals(1, clearedScrollIds.size());
    }

    @Test
    void clearsLatestScrollIdWhenNextPageFails() {
        pages = List.of(List.of("one"), List.of("two"));
        failScroll = true;
        Exception error = assertThrows(Exception.class, () -> task().run(runContextFactory.of()));
        assertTrue(error.toString().contains("scroll failed"), error.toString());
        assertEquals(List.of("scroll-0", "scroll-1"), requestedScrollIds);
        assertEquals(List.of("scroll-1"), clearedScrollIds);
    }

    @Test
    void cleanupFailureDoesNotMaskScrollFailure() {
        pages = List.of(List.of("one"), List.of("two"));
        failScroll = true;
        failClear = true;
        Exception error = assertThrows(Exception.class, () -> task().run(runContextFactory.of()));
        assertTrue(error.toString().contains("scroll failed"), error.toString());
        assertEquals(List.of("scroll-1"), clearedScrollIds);
    }
}
