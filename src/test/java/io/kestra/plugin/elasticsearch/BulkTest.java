package io.kestra.plugin.elasticsearch;

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

        Bulk put = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(upload(DATA.apply(indice)).toString())
            .chunk(Property.ofValue(10))
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(5L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(5D));
    }

    @Test
    void duplicateCreate_fails() throws Exception {
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var e = assertThrows(
            RuntimeException.class, () -> bulk(
                List.of(
                    Map.of("create", Map.of("_index", indice, "_id", "1")),
                    Map.of("field1", "value1"),
                    Map.of("create", Map.of("_index", indice, "_id", "1")),
                    Map.of("field1", "value2")
                )
            )
        );
        assertThat(e.getMessage(), containsString("version conflict"));
    }

    @Test
    void staleSeqNo_fails() throws Exception {
        var indice = seedPastSeqNoZero();

        var e = assertThrows(
            RuntimeException.class, () -> bulk(
                List.of(
                    Map.of("index", Map.of("_index", indice, "_id", "1", "if_seq_no", 0, "if_primary_term", 1)),
                    Map.of("field1", "stale")
                )
            )
        );
        assertThat(e.getMessage(), containsString("version conflict"));
    }

    @Test
    void staleSeqNoOnPlainUpdate_fails() throws Exception {
        var indice = seedPastSeqNoZero();

        var e = assertThrows(
            RuntimeException.class, () -> bulk(
                List.of(
                    Map.of("update", Map.of("_index", indice, "_id", "1", "if_seq_no", 0, "if_primary_term", 1)),
                    Map.of("field1", "stale")
                )
            )
        );
        assertThat(e.getMessage(), containsString("version conflict"));
    }

    // index then update a doc, so its seq_no advances to 1
    private String seedPastSeqNoZero() throws Exception {
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        bulk(
            List.of(
                Map.of("index", Map.of("_index", indice, "_id", "1")),
                Map.of("field1", "value1"),
                Map.of("update", Map.of("_index", indice, "_id", "1")),
                Map.of("doc", Map.of("field2", "value2"))
            )
        );
        return indice;
    }

    private void bulk(List<? extends Map<String, ?>> rows) throws Exception {
        Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(upload(rows).toString())
            .build()
            .run(runContextFactory.of());
    }

    private URI upload(List<? extends Map<String, ?>> rows) throws Exception {
        var tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (var row : rows) {
                output.write((JacksonMapper.ofJson().writeValueAsString(row) + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        try (var input = new FileInputStream(tempFile)) {
            return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }
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
