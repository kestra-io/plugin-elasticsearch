package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

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

        Scroll task = Scroll.builder()
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

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(1L));
    }

    @Test
    void runFull() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.of(Collections.singletonList("gbif")))
            .request("""
                {
                    "query": {
                        "match_all": {}
                    }
                }""")
            .build();

        Scroll.Output run = task.run(runContext);

        assertThat(run.getSize(), is(899L));
    }
}
