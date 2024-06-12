package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class SearchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        SearchSourceBuilder query = new SearchSourceBuilder();
        query.postFilter(QueryBuilders.termQuery("key", "925277090"));

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Collections.singletonList("gbif"))
            .request(query.toString())
            .build();

        Search.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1));
        assertThat(run.getRows().get(0).get("genericName"), is("Larus"));
    }

    @Test
    void runFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();

        SearchSourceBuilder query = new SearchSourceBuilder();
        query.query(QueryBuilders.termQuery("publishingCountry.keyword", "BE"));
        query.sort("key");

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Collections.singletonList("gbif"))
            .request(query.toString())
            .fetchType(FetchType.FETCH_ONE)
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

        SearchSourceBuilder query = new SearchSourceBuilder();
        query.query(QueryBuilders.termQuery("publishingCountry.keyword", "BE"));
        query.sort("key");

        Search task = Search.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Collections.singletonList("gbif"))
            .request(query.toString())
            .fetchType(FetchType.STORE)
            .build();

        Search.Output run = task.run(runContext);

        assertThat(run.getSize(), is(10));
        assertThat(run.getTotal(), is(28L));
        assertThat(run.getUri(), notNullValue());

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, run.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.get(8).get("key"), is(925311404));
    }
}
