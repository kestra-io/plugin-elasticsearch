package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class LoadTest extends ElsContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 100; i++) {
            FileSerde.write(output, ImmutableMap.of(
                "id", i,
                "name", "john"
            ));
        }
        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load put = Load.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.of(indice))
            .from(uri.toString())
            .chunk(Property.of(10))
            .idKey(Property.of("id"))
            .build();

        Load.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(100L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(10D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(100D));
    }
}
