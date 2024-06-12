package io.kestra.plugin.elasticsearch;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
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

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            output.write(("{ \"index\" : { \"_index\" : \"" + indice + "\", \"_id\" : \"1\" } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"field1\" : \"value1\" }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"delete\" : { \"_index\" : \"" + indice + "\", \"_id\" : \"1\" } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"create\" : { \"_index\" : \"" + indice + "\", \"_id\" : \"3\" } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"field1\" : \"value3\" }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"update\" : {\"_id\" : \"1\", \"_index\" : \"" + indice + "\"} }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"doc\" : {\"field2\" : \"value2\"} }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"create\" : { \"_index\" : \"" + indice + "\"}}\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ \"field1\" : \"value4\" }\n").getBytes(StandardCharsets.UTF_8));
        }

        URI uri = storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

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
