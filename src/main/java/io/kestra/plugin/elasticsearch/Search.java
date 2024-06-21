package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a search request.",
    description = "Get all documents from a search request and store it as outputs."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "indexes:",
                " - \"my_index\"",
                "request:",
                "  query: ",
                "    term:",
                "      name:",
                "        value: 'john'",
            }
        )
    }
)
public class Search extends AbstractSearch implements RunnableTask<Search.Output> {
    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    @PluginProperty
    private FetchType fetchType = FetchType.FETCH;

    @Override
    public Search.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (RestHighLevelClient client = this.connection.client(runContext)) {
            // build request
            SearchRequest request = this.request(runContext);
            logger.debug("Starting query: {}", request);

            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            Output.OutputBuilder outputBuilder = Search.Output.builder();

            switch (fetchType) {
                case FETCH:
                    Pair<List<Map<String, Object>>, Integer> fetch = this.fetch(searchResponse);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case FETCH_ONE:
                    var o = this.fetchOne(searchResponse);

                    outputBuilder
                        .row(o)
                        .size(o != null ? 1 : 0);
                    break;

                case STORE:
                    Pair<URI, Integer> store = this.store(runContext, searchResponse);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
                    break;
            }

            // metrics
            runContext.metric(Counter.of("requests.count", 1));
            runContext.metric(Counter.of("records", searchResponse.getHits().getHits().length));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(searchResponse.getTook().nanos())));

            // outputs
            return outputBuilder
                .total(searchResponse.getHits().getTotalHits().value)
                .build();
        }
    }


    protected Pair<URI, Integer> store(RunContext runContext, SearchResponse searchResponse) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new FileOutputStream(tempFile)) {
            Arrays
                .stream(searchResponse.getHits().getHits())
                .forEach(throwConsumer(docs -> FileSerde.write(output, docs.getSourceAsMap())));
        }

        return Pair.of(
            runContext.storage().putFile(tempFile),
            searchResponse.getHits().getHits().length
        );
    }

    protected Pair<List<Map<String, Object>>, Integer> fetch(SearchResponse searchResponse) {
        List<Map<String, Object>> result = new ArrayList<>();

        Arrays
            .stream(searchResponse.getHits().getHits())
            .forEach(throwConsumer(docs -> result.add(docs.getSourceAsMap())));

        return Pair.of(result, searchResponse.getHits().getHits().length);
    }

    protected Map<String, Object> fetchOne(SearchResponse searchResponse) {
        if (searchResponse.getHits().getHits().length == 0) {
            return null;
        }

        return searchResponse.getHits().getHits()[0].getSourceAsMap();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetched."
        )
        private Integer size;

        @Schema(
            title = "The total of the rows fetched without pagination."
        )
        private Long total;

        @Schema(
            title = "List containing the fetched data.",
            description = "Only populated if using `fetchType=FETCH`."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if using `fetchType=FETCH_ONE`."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The URI of the stored data.",
            description = "Only populated if using `fetchType=STORE`."
        )
        private URI uri;
    }
}
