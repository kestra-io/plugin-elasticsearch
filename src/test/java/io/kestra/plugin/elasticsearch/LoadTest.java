package io.kestra.plugin.elasticsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.elasticsearch.model.OpType;
import io.kestra.plugin.elasticsearch.shared.ElasticsearchConnection;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LoadTest extends ElsContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        var output = new FileOutputStream(tempFile);

        for (int i = 0; i < 100; i++) {
            FileSerde.write(
                output, ImmutableMap.of(
                    "id", i,
                    "name", "john"
                )
            );
        }
        var uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        var put = Load.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .from(uri.toString())
            .chunk(Property.ofValue(10))
            .idKey(Property.ofValue("id"))
            .build();

        var runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(100L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(10D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(100D));
    }

    @Test
    void opTypes() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var created = load(
            runContext, indice, OpType.CREATE, List.of(
                Map.of("id", "1", "name", "john", "city", "Paris"),
                Map.of("id", "2", "name", "jane", "city", "Lyon")
            )
        );
        assertThat(created.getSize(), is(2L));
        assertThat(get(runContext, indice, "1").get("name"), is("john"));

        load(
            runContext, indice, OpType.UPDATE, List.of(
                Map.of("id", "1", "name", "johnny")
            )
        );
        var updated = get(runContext, indice, "1");
        assertThat(updated.get("name"), is("johnny"));
        assertThat(updated.get("city"), is("Paris"));

        load(
            runContext, indice, OpType.DELETE, List.of(
                Map.of("id", "2")
            )
        );
        assertThat(get(runContext, indice, "2"), is(nullValue()));
    }

    @Test
    void indexOverwritesExistingDocument() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        load(runContext, indice, OpType.INDEX, List.of(Map.of("id", "1", "name", "john", "city", "Paris")));
        load(runContext, indice, OpType.INDEX, List.of(Map.of("id", "1", "name", "johnny")));

        var document = get(runContext, indice, "1");
        assertThat(document.get("name"), is("johnny"));
        assertThat(document.get("city"), is(nullValue()));
    }

    @Test
    void createOnExistingId_fails() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        load(runContext, indice, OpType.CREATE, List.of(Map.of("id", "1", "name", "john")));

        var e = assertThrows(
            RuntimeException.class,
            () -> load(runContext, indice, OpType.CREATE, List.of(Map.of("id", "1", "name", "johnny")))
        );
        assertThat(e.getMessage(), containsString("version conflict"));
        assertThat(get(runContext, indice, "1").get("name"), is("john"));
    }

    @Test
    void updateUpsertsMissingDocument() throws Exception {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        load(runContext, indice, OpType.UPDATE, List.of(Map.of("id", "1", "name", "john")));

        assertThat(get(runContext, indice, "1").get("name"), is("john"));
    }

    @Test
    void recordWithoutIdKey_throws() {
        var runContext = runContextFactory.of();
        var indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var e = assertThrows(
            IllegalArgumentException.class,
            () -> load(runContext, indice, OpType.UPDATE, List.of(Map.of("name", "john")))
        );
        assertThat(e.getMessage(), containsString("Record is missing idKey 'id'"));
    }

    @Test
    void deleteWithoutIdKey_throws() throws Exception {
        var runContext = runContextFactory.of();

        var load = Load.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue("ut_" + IdUtils.create().toLowerCase(Locale.ROOT)))
            .from(upload(List.of(Map.of("id", "1"))).toString())
            .opType(Property.ofValue(OpType.DELETE))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> load.run(runContext));
        assertThat(e.getMessage(), containsString("`idKey` is required"));
    }

    private Load.Output load(RunContext runContext, String indice, OpType opType, List<Map<String, Object>> rows) throws Exception {
        return Load.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .from(upload(rows).toString())
            .idKey(Property.ofValue("id"))
            .opType(Property.ofValue(opType))
            .build()
            .run(runContext);
    }

    private Map<String, Object> get(RunContext runContext, String indice, String id) throws Exception {
        return Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .key(Property.ofValue(id))
            .build()
            .run(runContext)
            .getRow();
    }

    private URI upload(List<Map<String, Object>> rows) throws Exception {
        var tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (var row : rows) {
                FileSerde.write(output, row);
            }
        }
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
