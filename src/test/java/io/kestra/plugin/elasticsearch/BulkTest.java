package io.kestra.plugin.elasticsearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.elasticsearch.shared.ElasticsearchConnection;

import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BulkTest extends ElsContainer {

    @Inject
    private StorageInterface storageInterface;

    private static final Function<String, List<Map<String, Object>>> DATA = (String indice) -> List.of(
        Map.of("index", Map.of("_index", indice, "_id", "1")),
        Map.of("field1", "value1"),
        Map.of("delete", Map.of("_index", indice, "_id", "1")),
        Map.of("create", Map.of("_index", indice, "_id", "3")),
        Map.of("field1", "value3"),
        Map.of("update", Map.of("_index", indice, "_id", "1")),
        Map.of("doc", Map.of("field2", "value2"), "doc_as_upsert", true),
        Map.of("create", Map.of("_index", indice)),
        Map.of("field1", "value4")
    );

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            DATA.apply(indice)
                .forEach(
                    throwConsumer(
                        s -> output.write(
                            (JacksonMapper
                                .ofJson()
                                .writeValueAsString(s) + "\n")
                                .getBytes(StandardCharsets.UTF_8)
                        )
                    )
                );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .chunk(Property.ofValue(10))
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(5L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(5D));
    }

    @Test
    void duplicateCreate_fails() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            List.of(
                Map.of("create", Map.of("_index", indice, "_id", "1")),
                Map.of("field1", "value1"),
                Map.of("create", Map.of("_index", indice, "_id", "1")),
                Map.of("field1", "value2")
            )
                .forEach(
                    throwConsumer(
                        s -> output.write(
                            (JacksonMapper
                                .ofJson()
                                .writeValueAsString(s) + "\n")
                                .getBytes(StandardCharsets.UTF_8)
                        )
                    )
                );
        }

        var uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        var bulk = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .build();

        var e = assertThrows(RuntimeException.class, () -> bulk.run(runContext));
        assertThat(e.getMessage(), containsString("version conflict"));
    }

    @Test
    void staleConditionalWrites_failWithoutChangingDocument() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        bulk(runContext, List.of(Map.of("index", Map.of("_index", indice, "_id", "1")), Map.of("status", "old")));
        var original = elasticsearchClient.get(b -> b.index(indice).id("1"), Map.class);
        bulk(runContext, List.of(Map.of("index", Map.of("_index", indice, "_id", "1")), Map.of("status", "newer")));

        for (var type : List.of("index", "update", "delete")) {
            var header = Map.<String, Object> of(
                type, Map.of("_index", indice, "_id", "1", "if_seq_no", original.seqNo(), "if_primary_term", original.primaryTerm())
            );
            var rows = type.equals("delete") ? List.of(header)
                : List.of(
                    header, type.equals("update") ? Map.<String, Object> of("doc", Map.of("status", "stale")) : Map.<String, Object> of("status", "stale")
                );
            var e = assertThrows(RuntimeException.class, () -> bulk(runContext, rows));
            assertThat(e.getMessage(), containsString("version conflict"));
            assertThat(elasticsearchClient.get(b -> b.index(indice).id("1"), Map.class).source(), is(Map.of("status", "newer")));
        }
    }

    @Test
    void nativeUpdate_changesTopLevelFieldAndPreservesOtherFields() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        bulk(runContext, List.of(Map.of("index", Map.of("_index", indice, "_id", "1")), Map.of("status", "old", "keep", "yes")));

        var output = bulk(runContext, List.of(Map.of("update", Map.of("_index", indice, "_id", "1")), Map.of("doc", Map.of("status", "new"))));

        assertThat(output.getSize(), is(1L));
        assertThat(elasticsearchClient.get(b -> b.index(indice).id("1"), Map.class).source(), is(Map.of("status", "new", "keep", "yes")));
    }

    @Test
    void nativeUpdate_doesNotUpsertUnlessRequested() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        bulk(runContext, List.of(Map.of("index", Map.of("_index", indice, "_id", "1")), Map.of("status", "old")));
        var header = Map.<String, Object> of("update", Map.of("_index", indice, "_id", "2"));

        var e = assertThrows(RuntimeException.class, () -> bulk(runContext, List.of(header, Map.of("doc", Map.of("status", "new")))));
        assertThat(e.getMessage(), containsString("document missing"));
        assertThat(elasticsearchClient.get(b -> b.index(indice).id("2"), Map.class).source(), is(nullValue()));

        bulk(runContext, List.of(header, Map.of("doc", Map.of("status", "new"), "doc_as_upsert", true)));
        assertThat(elasticsearchClient.get(b -> b.index(indice).id("2"), Map.class).source(), is(Map.of("status", "new")));
    }

    @Test
    void nativeScriptAndUpsert_areExecuted() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        var rows = List.of(
            Map.<String, Object> of("update", Map.of("_index", indice, "_id", "1")),
            Map.<String, Object> of("script", Map.of("source", "ctx._source.count += params.n", "params", Map.of("n", 2)), "upsert", Map.of("count", 0))
        );

        bulk(runContext, rows);
        bulk(runContext, rows);

        assertThat(elasticsearchClient.get(b -> b.index(indice).id("1"), Map.class).source(), is(Map.of("count", 2)));
    }

    @Test
    void legacyPlainUpdate_stillUpserts() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        var output = bulk(runContext, List.of(Map.of("update", Map.of("_index", indice, "_id", "1")), Map.of("status", "new")));

        assertThat(output.getSize(), is(1L));
        assertThat(elasticsearchClient.get(b -> b.index(indice).id("1"), Map.class).source(), is(Map.of("status", "new")));
    }

    private Bulk.Output bulk(RunContext runContext, List<Map<String, Object>> rows) throws Exception {
        var content = new StringBuilder();
        for (var row : rows) {
            content.append(JacksonMapper.ofJson().writeValueAsString(row)).append('\n');
        }
        var uri = storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ndjson"),
            new ByteArrayInputStream(content.toString().getBytes(StandardCharsets.UTF_8))
        );
        return Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .build()
            .run(runContext);
    }

    @Test
    void runIon() throws Exception {
        RunContext runContext = runContextFactory.of();

        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            DATA.apply(indice)
                .forEach(throwConsumer(s -> FileSerde.write(output, s)));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .chunk(Property.ofValue(10))
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(5L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(5D));
    }
}
