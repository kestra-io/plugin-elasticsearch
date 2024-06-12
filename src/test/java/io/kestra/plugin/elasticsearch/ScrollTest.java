package io.kestra.plugin.elasticsearch;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ScrollTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        SearchSourceBuilder query = new SearchSourceBuilder();
        query.query(QueryBuilders.termQuery("key", "925277090"));

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Collections.singletonList("gbif"))
            .request(query.toString())
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1L));
    }

    @Test
    void runFull() throws Exception {
        RunContext runContext = runContextFactory.of();

        SearchSourceBuilder query = new SearchSourceBuilder();
        query.query(QueryBuilders.matchAllQuery());

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Collections.singletonList("gbif"))
            .request(query.toString())
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(899L));
    }
}
