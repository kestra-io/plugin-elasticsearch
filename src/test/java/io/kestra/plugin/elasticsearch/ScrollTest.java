package io.kestra.plugin.elasticsearch;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.elasticsearch.shared.ElasticsearchConnection;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ScrollTest extends ElsContainer {
    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "term": {
                            "key": "925277090"
                        }
                    }
                }""")
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1L));
        assertThat(readRows(run).getFirst().get("genericName"), is("Larus"));
    }

    @Test
    void runFull() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
            .request("""
                {
                    "size": 128,
                    "query": {
                        "match_all": {}
                    }
                }""")
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(899L));
        List<Map<?, ?>> rows = readRows(run);
        assertThat(rows.size(), is(899));
        assertThat(rows.stream().map(row -> row.get("key")).distinct().count(), is(899L));
    }

    @Test
    void runEmpty() throws Exception {
        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
            .request("{\"query\":{\"match_none\":{}}}")
            .build();

        Scroll.Output output = task.run(runContextFactory.of());
        assertThat(output.getSize(), is(0L));
        assertThat(readRows(output).size(), is(0));
    }

    private List<Map<?, ?>> readRows(Scroll.Output output) throws Exception {
        List<Map<?, ?>> rows = new ArrayList<>();
        try (var input = new BufferedInputStream(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()))) {
            FileSerde.read(input, row -> rows.add((Map<?, ?>) row));
        }
        return rows;
    }
}
