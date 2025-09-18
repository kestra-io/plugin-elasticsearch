package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class ScrollTest extends ElsContainer {

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
    }

    @Test
    void runFull() throws Exception {
        RunContext runContext = runContextFactory.of();

        Scroll task = Scroll.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .indexes(Property.ofValue(Collections.singletonList("gbif")))
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
