package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.elasticsearch.model.HttpMethod;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestTest extends ElsContainer {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${elasticsearch-hosts}")
    private List<String> hosts;


    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String indice = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        Request request = Request.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .method(Property.of(HttpMethod.POST))
            .endpoint(Property.of(indice + "/_doc/" + IdUtils.create()))
            .parameters(Property.of(Map.of("human", "true")))
            .body(Map.of("name", "john"))
            .build();


        Request.Output runOutput = request.run(runContext);

        assertThat(((Map<String, String>) runOutput.getResponse()).get("_index"), is(indice));
    }

    @Test
    void cat() throws Exception {
        RunContext runContext = runContextFactory.of();

        Request request = Request.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .method(Property.of(HttpMethod.GET))
            .endpoint(Property.of("_cat/indices"))
            .build();

        Request.Output runOutput = request.run(runContext);

        assertThat(((String) runOutput.getResponse()).contains("open"), is(true));
    }
}
