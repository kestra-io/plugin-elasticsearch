package io.kestra.plugin.elasticsearch;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class SearchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;

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

        assertThat(run.getSize(), is(1L));
        assertThat(run.getRows().get(0).get("genericName"), is("Larus"));
    }
}
