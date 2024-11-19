package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.esql.objects.ObjectsEsqlAdapter;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.esql.QueryRequest;
import co.elastic.clients.elasticsearch.esql.query.EsqlFormat;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterables;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a ES|QL search request.",
    description = "Get documents from a ES|QL search request and store it as outputs."
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
                    query: |
                      FROM library
                      | KEEP author, name, page_count, release_date
                      | SORT page_count DESC
                      | LIMIT 5
                """
        )
    }
)
public class Esql extends AbstractTask implements RunnableTask<Esql.Output> {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {};

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    @PluginProperty
    @NotNull
    private FetchType fetchType = FetchType.FETCH;

    @Schema(
        title = "The ElasticSearch value.",
        description = "Can be a JSON string. In this case, the contentType will be used or a raw Map."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String query;

    @Schema(
        title = "A Query DSL query filter.",
        description = "Specify a Query DSL query in the filter parameter to filter the set of documents that an ES|QL query runs on."
    )
    @PluginProperty(dynamic = true)
    private Object filter;

    @Override
    public Esql.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (RestClientTransport transport = this.connection.client(runContext)) {
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // build request
            QueryRequest queryRequest = QueryRequest.of(throwFunction(builder -> {
                    builder.query(runContext.render(this.query));
                    builder.format(EsqlFormat.Json);
                    builder.columnar(false);

                    if (filter != null) {
                        SearchRequest.Builder request = QueryService.request(runContext, this.filter);
                        builder.filter(request.build().query());
                    }

                    return builder;
                }
            ));

            logger.debug("Starting query: {}", query);

            Iterable<Map<String, Object>> queryResponse = client
                .esql()
                .query(ForkObjectsEsqlAdapter.of(TYPE_REFERENCE.getType()), queryRequest);

            Output.OutputBuilder outputBuilder = Esql.Output.builder();

            switch (fetchType) {
                case FETCH:
                    Pair<List<Map<String, Object>>, Integer> fetch = this.fetch(queryResponse);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case FETCH_ONE:
                    var o = this.fetchOne(queryResponse);

                    outputBuilder
                        .row(o)
                        .size(o != null ? 1 : 0);
                    break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, queryResponse);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight().intValue());
                    break;
            }

            int size = Iterables.size(queryResponse);

            runContext.metric(Counter.of("records", size));
            outputBuilder.total((long) size);

            // metrics
            runContext.metric(Counter.of("requests.count", 1));

            // outputs
            return outputBuilder
                .build();
        }
    }


    protected Pair<URI, Long> store(RunContext runContext, Iterable<Map<String, Object>> searchResponse) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            Flux<Map<String, Object>> hitFlux = Flux.fromIterable(searchResponse);
            Long count = FileSerde.writeAll(output, hitFlux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    protected Pair<List<Map<String, Object>>, Integer> fetch(Iterable<Map<String, Object>> searchResponse) {
        List<Map<String, Object>> result = StreamSupport.stream(searchResponse.spliterator(), false)
            .collect(Collectors.toList());

        return Pair.of(result, result.size());
    }

    protected Map<String, Object> fetchOne(Iterable<Map<String, Object>> searchResponse) {
        if (!searchResponse.iterator().hasNext()) {
            return null;
        }

        return searchResponse.iterator().next();
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
