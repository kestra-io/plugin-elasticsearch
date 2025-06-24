package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PutGetTest extends ElsContainer {

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", Map.of("name", "John Doe")));
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        Put put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.of(indice))
            .value("{{ variable }}")
            .build();

        Put.Output putOutput = put.run(runContext);

        Get task = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.of(indice))
            .key(Property.of(putOutput.getId()))
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRow().get("name"), is("John Doe"));

        put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.of(indice))
            .value(Map.of(
                "name", "Jane Doe"
            ))
            .build();

        putOutput = put.run(runContext);

        task = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.of(indice))
            .key(Property.of(putOutput.getId()))
            .build();

        runOutput = task.run(runContext);

        assertThat(runOutput.getRow().get("name"), is("Jane Doe"));
    }


    @Test
    void shouldThrowWhenDocumentNotFound() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", Map.of("name", "John Doe")));
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        Put put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .value("{{ variable }}")
            .build();

        put.run(runContext);

        Get get = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .key(Property.ofValue(IdUtils.create()))
            .errorOnMissing(Property.ofValue(true))
            .build();

        assertThrows(IllegalStateException.class, () -> {
            get.run(runContext);
        });
    }

    @Test
    void shouldSucceedWhenDocumentNotFound() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", Map.of("name", "John Doe")));
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        Put put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .value("{{ variable }}")
            .build();

        put.run(runContext);

        Get get = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(Property.ofValue(indice))
            .key(Property.ofValue(IdUtils.create()))
            .errorOnMissing(Property.ofValue(false))
            .build();

        Get.Output runOutput = get.run(runContext);

        assertThat(runOutput.getRow(), is(nullValue()));
    }
}

