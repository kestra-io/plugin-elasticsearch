package io.kestra.plugin.elasticsearch;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class PutGetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", Map.of("name", "John Doe")));
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        Put put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(indice)
            .value("{{ variable }}")
            .build();

        Put.Output putOutput = put.run(runContext);

        Get task = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(indice)
            .key(putOutput.getId())
            .build();

        Get.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRow().get("name"), is("John Doe"));

        put = Put.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(indice)
            .value(Map.of(
                "name", "Jane Doe"
            ))
            .build();

        putOutput = put.run(runContext);

        task = Get.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .index(indice)
            .key(putOutput.getId())
            .build();

        runOutput = task.run(runContext);

        assertThat(runOutput.getRow().get("name"), is("Jane Doe"));
    }
}
