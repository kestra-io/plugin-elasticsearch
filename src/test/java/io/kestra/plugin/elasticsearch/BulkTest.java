package io.kestra.plugin.elasticsearch;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

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

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class BulkTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

    @Inject
    private StorageInterface storageInterface;

    private static final Function<String, List<Map<String, Object>>> DATA = (String indice) -> List.of(
        Map.of("index", Map.of("_index", indice, "_id", "1")),
        Map.of("field1", "value1"),
        Map.of("delete", Map.of("_index", indice, "_id", "1")),
        Map.of("create", Map.of("_index", indice, "_id", "3")),
        Map.of("field1", "value3"),
        Map.of("update", Map.of("_index", indice, "_id", "1")),
        Map.of("doc", Map.of("field2", "value2")),
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
                .forEach(throwConsumer(s -> output.write((JacksonMapper
                    .ofJson()
                    .writeValueAsString(s) + "\n")
                    .getBytes(StandardCharsets.UTF_8)
                )));
        }

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .chunk(10)
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(5L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(5D));
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

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .from(uri.toString())
            .chunk(10)
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(5L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(5D));
    }
}
