package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.esql.EsqlAdapter;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.esql.ElasticsearchEsqlAsyncClient;
import co.elastic.clients.elasticsearch.esql.EsqlFormat;
import co.elastic.clients.elasticsearch.esql.QueryRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterables;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run ES|QL query",
    description = "Executes an ES|QL query and returns hits in different formats. Defaults to `fetchType=FETCH`; STORE writes results to Kestra internal storage. Only the current result set is processed—no pagination."
)
@Plugin(
    metrics = {
        @Metric(name = "requests.count", type = Counter.TYPE, description = "Number of ES|QL requests sent"),
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of records returned")
    },
    examples = {
        @Example(
            title = "Load data in bulk to Elasticsearch and query it using ES|QL.",
            full = true,
            code = """
                id: bulk_load_and_query
                namespace: company.team

                tasks:
                  - id: extract
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/jsonl/books.jsonl

                  - id: load
                    type: io.kestra.plugin.elasticsearch.Bulk
                    from: "{{ outputs.extract.uri }}"

                  - id: sleep
                    type: io.kestra.plugin.core.flow.Sleep
                    duration: PT5S
                    description: Pause needed after load before we can query

                  - id: query
                    type: io.kestra.plugin.elasticsearch.Esql
                    fetchType: STORE
                    query: |
                      FROM books
                        | KEEP author, name, page_count, release_date
                        | SORT page_count DESC
                        | LIMIT 5

                pluginDefaults:
                  - type: io.kestra.plugin.elasticsearch
                    values:
                      connection:
                        headers:
                          - "Authorization: ApiKey yourEncodedApiKey"
                        hosts:
                          - https://yourCluster.us-central1.gcp.cloud.es.io:443
                """
        )
    }
)
public class Esql extends AbstractTask implements RunnableTask<Esql.Output> {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {
    };

    @Schema(
        title = "Result handling mode",
        description = "Controls how query results are exposed; default `FETCH` returns all rows. `FETCH_ONE` returns the first row, `STORE` writes rows to Kestra storage and returns a URI, `NONE` leaves outputs empty."
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "ES|QL query string",
        description = "ES|QL statement rendered at runtime; required."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Query filter",
        description = "Optional DSL filter applied before the ES|QL query runs."
    )
    @PluginProperty(dynamic = true, group = "processing")
    private Object filter;

    @Schema(
        title = "Query params",
        description = "Optional parameters. Available from 9.1 ES clusters. Using the anonymous syntax : add a question mark for each parameter in your query. It will be taken in same order as in parameter list"
    )
    @PluginProperty(dynamic = true, group = "processing")
    private List<String> params;

    @Builder.Default
    @Schema(
        title = "Columnar result",
        description = "Optional boolean to choose results in columnar way or not. Default is false"
    )
    @PluginProperty(dynamic = true, group = "destination")
    private Property<Boolean> columnar = Property.ofValue(false);


    @Builder.Default
    @Schema(
        title = "Async query",
        description = "Optional boolean to choose to run query using the async endpoint of Elasticsearch. Default is false"
    )
    @PluginProperty(dynamic = true, group = "connection")
    private Property<Boolean> async = Property.ofValue(false);


    @Override
    public Esql.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (ElasticsearchClient client = this.connection.highLevelClient(runContext)) {
            // build request
            Boolean rColumnar = runContext.render(this.columnar).as(Boolean.class).orElseThrow();
            QueryRequest queryRequest = QueryRequest.of(throwFunction(builder ->
                {
                    builder.query(runContext.render(this.query).as(String.class).orElseThrow());
                    builder.format(EsqlFormat.Json);
                    builder.columnar(rColumnar);

                    if (filter != null) {
                        SearchRequest.Builder request = QueryService.request(runContext, this.filter);
                        builder.filter(request.build().query());
                    }

                    if (params != null) {
                        for (String param : params) {
                            addToParams(param, builder);
                        }
                    }

                    return builder;
                }
            ));


            logger.debug("Starting query: {}", query);

            EsqlAdapter<Iterable<Map<String, Object>>> adapter = ForkObjectsEsqlAdapter.of(TYPE_REFERENCE.getType());

            if (rColumnar) {
                adapter = ColumnarForkObjectsEsqlAdapter.of(TYPE_REFERENCE.getType());
            }

            Iterable<Map<String, Object>> queryResponse;
            if (runContext.render(this.async).as(Boolean.class).orElseThrow()) {
                // Warning : use of internal method `_transport` because it is not possible to create an ElasticsearchEsqlAsyncClient from a ElasticsearchClient
                ElasticsearchEsqlAsyncClient esqlAsyncClient =
                    new ElasticsearchAsyncClient(client._transport()).esql();
                CompletableFuture<Iterable<Map<String, Object>>> queryResponseCompletableFuture = esqlAsyncClient.query(ForkObjectsEsqlAdapter.of(TYPE_REFERENCE.getType()), queryRequest);
                queryResponse = queryResponseCompletableFuture.get();
            } else {
                queryResponse = client
                    .esql()
                    .query(adapter, queryRequest);
            }

            Output.OutputBuilder outputBuilder = Esql.Output.builder();

            switch (runContext.render(this.fetchType).as(FetchType.class).orElseThrow()) {
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

    private void addToParams(String object, QueryRequest.Builder builder) {
        Object parsed = parse(object);
        // `builder.params()` has several implementations with parameter override. Need to find the corresponding type.
        switch (parsed) {
            case Integer i -> builder.params(i);
            case Long l -> builder.params(l);
            case Double d -> builder.params(d);
            case Boolean b -> builder.params(b);
            case String str -> builder.params(str);
            default -> throw new IllegalArgumentException("Invalid parameter type on '" + object + "' and type '" + parsed.getClass() + "'");
        }

    }

    private Object parse(String s) {
        if (s == null) return null;
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(s);
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ignored) {
        }
        return s;  // fallback: keep as string
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
            title = "Returned row count",
            description = "Number of rows included in outputs for the selected fetch type."
        )
        private Integer size;

        @Schema(
            title = "Total rows reported",
            description = "Total rows returned by the ES|QL response."
        )
        private Long total;

        @Schema(
            title = "Fetched rows",
            description = "Populated when `fetchType=FETCH`; contains all rows from the response."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First row",
            description = "Populated when `fetchType=FETCH_ONE`; contains the first row only."
        )
        private Map<String, Object> row;

        @Schema(
            title = "Stored rows URI",
            description = "Populated when `fetchType=STORE`; Kestra internal storage path to the Ion file."
        )
        private URI uri;
    }
}
