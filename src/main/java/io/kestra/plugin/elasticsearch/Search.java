package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a search request to an Elasticsearch cluster.",
    description = "Get all documents from a search request and store it as outputs."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: elasticsearch_search
                namespace: company.team

                tasks:
                  - id: search
                    type: io.kestra.plugin.elasticsearch.Search
                    connection:
                      hosts:
                        - "http://localhost:9200"
                    indexes:
                      - "my_index"
                    request:
                      query:
                        term:
                          name:
                            value: 'john'
                """
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
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Search.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (RestClientTransport transport = this.connection.client(runContext)) {
            ElasticsearchClient client = new ElasticsearchClient(transport);
            // build request
            SearchRequest.Builder request = this.request(runContext);
            logger.debug("Starting query: {}", request);

            SearchResponse<Map> searchResponse = client.search(request.build(), Map.class);

            Output.OutputBuilder outputBuilder = Search.Output.builder();

            switch (runContext.render(fetchType).as(FetchType.class).orElseThrow()) {
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
                    Pair<URI, Long> store = this.store(runContext, searchResponse);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight().intValue());
                    break;
            }

            // metrics
            runContext.metric(Counter.of("requests.count", 1));
            runContext.metric(Counter.of("records", searchResponse.hits().hits().size()));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(searchResponse.took())));

            // outputs
            return outputBuilder
                .total(searchResponse.hits().total().value())
                .build();
        }
    }


    protected Pair<URI, Long> store(RunContext runContext, SearchResponse<Map> searchResponse) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map> hitFlux = Flux.fromIterable(searchResponse.hits().hits()).map(hit -> hit.source());
            Long count = FileSerde.writeAll(output, hitFlux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    protected Pair<List<Map<String, Object>>, Integer> fetch(SearchResponse<Map> searchResponse) {
        List<Map<String, Object>> result = new ArrayList<>();

        searchResponse.hits().hits()
            .forEach(throwConsumer(docs -> result.add(docs.source())));

        return Pair.of(result, searchResponse.hits().hits().size());
    }

    protected Map<String, Object> fetchOne(SearchResponse<Map> searchResponse) {
        if (searchResponse.hits().hits().isEmpty()) {
            return null;
        }

        return searchResponse.hits().hits().getFirst().source();
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
