package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EsqlTest extends ElsContainer{
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        Esql task = Esql.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .query(Property.of("""
                FROM gbif
                | WHERE key == 925277090
                """))
            .build();

        Esql.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getRows().getFirst().get("genericName"), is("Larus"));
    }

    @Test
    void filter() throws Exception {
        RunContext runContext = runContextFactory.of();

        Esql task = Esql.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .query(Property.of("FROM gbif"))
            .filter("""
                {
                    "query": {
                        "term": {
                            "key": "925277090"
                        }
                    }
                }""")
            .build();

        Esql.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getRows().getFirst().get("genericName"), is("Larus"));
    }

    @Test
    void runFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();

        Esql task = Esql.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .query(Property.of("""
                FROM gbif
                | WHERE publishingCountry.keyword == "BE"
                """))
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build();

        Esql.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getTotal(), is(28L));
        assertThat(run.getRow().get("key"), is(925277090L));
    }

    @SuppressWarnings("unchecked")
    @Test
    void runStored() throws Exception {
        RunContext runContext = runContextFactory.of();

        Esql task = Esql.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .query(Property.of("""
                FROM gbif
                | WHERE publishingCountry.keyword == "BE"
                | LIMIT 10
                """))
            .fetchType(Property.of(FetchType.STORE))
            .build();

        Esql.Output run = task.run(runContext);

        assertThat(run.getSize(), is(10));
        assertThat(run.getTotal(), is(10L));
        assertThat(run.getUri(), notNullValue());

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, null, run.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.get(8).get("key"), is(925311404));
    }
}
