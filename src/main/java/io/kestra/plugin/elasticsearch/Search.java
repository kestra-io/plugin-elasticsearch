package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.aggregations.Aggregation;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a search request",
    description = "Get all documents from a search request and store it as outputs"
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
    @Override
    public Search.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (RestHighLevelClient client = this.connection.client(runContext)) {
            // build request
            SearchRequest request = this.request(runContext);
            logger.debug("Starting query: {}", request);

            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            // fetch
            ArrayList<Map<String, Object>> rows = new ArrayList<>();
            AtomicLong recordsCount = new AtomicLong();

            Arrays.stream(searchResponse.getHits().getHits())
                .forEach(throwConsumer(documentFields -> {
                    recordsCount.incrementAndGet();
                    rows.add(documentFields.getSourceAsMap());
                }));

            // metrics
            runContext.metric(Counter.of("requests.count", 1));
            runContext.metric(Counter.of("records", searchResponse.getHits().getHits().length));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(searchResponse.getTook().nanos())));

            // outputs
            return Output.builder()
                .size(recordsCount.get())
                .total(searchResponse.getHits().getTotalHits().value)
                .rows(rows)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetch"
        )
        private Long size;

        @Schema(
            title = "The total of the rows fetch without pagination"
        )
        private Long total;

        @Schema(
            title = "The search result fetch"
        )
        private List<Map<String, Object>> rows;
    }
}
