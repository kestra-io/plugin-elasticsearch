package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SearchTest extends ElsContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.of(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "term": {
                            "key": "925277090"
                        }
                    }
                }""")
            .build();

        Search.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getRows().get(0).get("genericName"), is("Larus"));
    }

    @Test
    void runFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.of(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "term": {
                            "publishingCountry.keyword": "BE"
                        }
                    },
                    "sort": {
                        "key": "asc"
                    }
                }""")
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build();

        Search.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getTotal(), is(28L));
        assertThat(run.getRow().get("key"), is(925277090));
    }

    @SuppressWarnings("unchecked")
    @Test
    void runStored() throws Exception {
        RunContext runContext = runContextFactory.of();

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.of(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "term": {
                            "publishingCountry.keyword": "BE"
                        }
                    }
                }""")
            .fetchType(Property.of(FetchType.STORE))
            .build();

        Search.Output run = task.run(runContext);

        assertThat(run.getSize(), is(10));
        assertThat(run.getTotal(), is(28L));
        assertThat(run.getUri(), notNullValue());

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.get(8).get("key"), is(925311404));
    }
}
