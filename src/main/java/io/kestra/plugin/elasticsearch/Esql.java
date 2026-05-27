package io.kestra.plugin.elasticsearch;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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
import io.kestra.core.serializers.JacksonMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.esql.EsqlAdapter;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.esql.EsqlFormat;
import co.elastic.clients.elasticsearch.esql.QueryRequest;
import co.elastic.clients.json.JsonpUtils;
import co.elastic.clients.transport.endpoints.BinaryResponse;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

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
        description = "Optional parameters. Add a question mark (`?`) for each parameter in your query, in order. Parameters are interpolated client-side before the query is sent"
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> params;

    @Builder.Default
    @Schema(
        title = "Columnar result",
        description = "Optional boolean to choose results in columnar way or not. Default is false"
    )
    @PluginProperty(group = "processing")
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
            Boolean rColumnar = runContext.render(this.columnar).as(Boolean.class).orElse(false);
            String renderedQuery = runContext.render(this.query).as(String.class).orElseThrow();
            //noinspection unchecked
            List<String> rParams = this.params == null
                ? List.of()
                : runContext.render(this.params).as((Class<List<String>>) (Class<?>) List.class).orElse(List.of());
            String finalQuery = interpolateParams(renderedQuery, rParams);

            QueryRequest queryRequest = QueryRequest.of(throwFunction(builder ->
            {
                builder.query(finalQuery);
                builder.format(EsqlFormat.Json);
                builder.columnar(rColumnar);

                if (filter != null) {
                    SearchRequest.Builder request = QueryService.request(runContext, this.filter);
                    builder.filter(request.build().query());
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
            if (runContext.render(this.async).as(Boolean.class).orElse(false)) {
                queryResponse = runAsyncQuery(runContext, client, queryRequest, adapter, logger);
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

    private static final String ASYNC_KEEP_ALIVE = "5m";
    private static final String ASYNC_POLL_TIMEOUT = "30s";

    private Iterable<Map<String, Object>> runAsyncQuery(
        RunContext runContext,
        ElasticsearchClient client,
        QueryRequest queryRequest,
        EsqlAdapter<Iterable<Map<String, Object>>> adapter,
        Logger logger) throws Exception {
        String body = buildAsyncBody(JsonpUtils.toJsonString(queryRequest, client._jsonpMapper()));

        try (Rest5Client lowLevel = this.connection.client(runContext)) {
            byte[] responseBytes = submitAsyncQuery(lowLevel, body);
            JsonNode response = JacksonMapper.ofJson().readTree(responseBytes);
            String asyncId = response.path("id").isMissingNode() ? null : response.path("id").asText();

            try {
                while (response.path("is_running").asBoolean(false)) {
                    if (asyncId == null) {
                        throw new IllegalStateException("ES|QL async response is still running but did not return an id");
                    }
                    logger.debug("Polling ES|QL async query id={}", asyncId);
                    responseBytes = pollAsyncQuery(lowLevel, asyncId);
                    response = JacksonMapper.ofJson().readTree(responseBytes);
                }

                return adapter.deserialize(client.esql(), queryRequest, bufferedBinaryResponse(responseBytes));
            } finally {
                if (asyncId != null) {
                    deleteAsyncQuery(lowLevel, asyncId, logger);
                }
            }
        }
    }

    private byte[] submitAsyncQuery(Rest5Client lowLevel, String body) throws IOException {
        var request = new co.elastic.clients.transport.rest5_client.low_level.Request("POST", "_query/async");
        request.setJsonEntity(body);
        return readBody(lowLevel.performRequest(request));
    }

    private static String buildAsyncBody(String queryRequestJson) throws IOException {
        var mapper = JacksonMapper.ofJson();
        com.fasterxml.jackson.databind.node.ObjectNode body = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(queryRequestJson);
        body.put("wait_for_completion_timeout", "0s");
        body.put("keep_alive", ASYNC_KEEP_ALIVE);
        return mapper.writeValueAsString(body);
    }

    private byte[] pollAsyncQuery(Rest5Client lowLevel, String id) throws IOException {
        var request = new co.elastic.clients.transport.rest5_client.low_level.Request("GET", "_query/async/" + id);
        request.addParameter("wait_for_completion_timeout", ASYNC_POLL_TIMEOUT);
        return readBody(lowLevel.performRequest(request));
    }

    private void deleteAsyncQuery(Rest5Client lowLevel, String id, Logger logger) {
        try {
            lowLevel.performRequest(new co.elastic.clients.transport.rest5_client.low_level.Request("DELETE", "_query/async/" + id));
        } catch (Exception e) {
            logger.warn("Failed to delete async ES|QL query {}", id, e);
        }
    }

    private static byte[] readBody(co.elastic.clients.transport.rest5_client.low_level.Response response) throws IOException {
        return IOUtils.toByteArray(response.getEntity().getContent());
    }

    private static BinaryResponse bufferedBinaryResponse(byte[] body) {
        return new BinaryResponse() {
            private final InputStream stream = new ByteArrayInputStream(body);

            @Override
            public String contentType() {
                return "application/json";
            }

            @Override
            public long contentLength() {
                return body.length;
            }

            @Override
            public InputStream content() {
                return stream;
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Matches one ES|QL token at a time so {@link Matcher#appendReplacement} can rewrite only the `?` placeholders
     * while preserving any `?` that happens to live inside a string literal or a backticked identifier.
     * see <a href="https://www.elastic.co/docs/reference/query-languages/esql/esql-rest#esql-rest-identifier-params">ES documentation</a>
     */
    private static final Pattern QUERY_TOKEN = Pattern.compile(
        "\"\"\".*?\"\"\"" // triple-quoted raw string
            + "|\"(?:\\\\.|[^\"\\\\])*\"" // double-quoted string with backslash escapes
            + "|`[^`]*`" // backtick-quoted identifier
            + "|\\?", // anonymous placeholder
        Pattern.DOTALL
    );

    private static String interpolateParams(String query, List<String> params) {
        if (params.isEmpty()) {
            return query;
        }

        Matcher matcher = QUERY_TOKEN.matcher(query);
        StringBuilder out = new StringBuilder(query.length() + params.size() * 8);
        int paramIdx = 0;

        while (matcher.find()) {
            String token = matcher.group();
            String replacement;
            if (token.equals("?")) {
                if (paramIdx >= params.size()) {
                    throw new IllegalArgumentException("Query contains more `?` placeholders than the provided params list (" + params.size() + ")");
                }
                replacement = formatParamLiteral(params.get(paramIdx++));
            } else {
                replacement = token;
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);

        if (paramIdx < params.size()) {
            throw new IllegalArgumentException("Provided params list has " + params.size() + " entries but the query only contains " + paramIdx + " `?` placeholders");
        }

        return out.toString();
    }

    private static String formatParamLiteral(String raw) {
        Object parsed = parse(raw);
        return switch (parsed) {
            case Integer i -> Integer.toString(i);
            case Long l -> Long.toString(l);
            case Double d -> Double.toString(d);
            case Boolean b -> Boolean.toString(b);
            case String s -> "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
            case null -> "null";
            default -> throw new IllegalArgumentException("Invalid parameter type on '" + raw + "' and type '" + parsed.getClass() + "'");
        };
    }

    private static Object parse(String s) {
        if (s == null)
            return null;
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
        return s; // fallback: keep as string
    }

    protected Pair<URI, Long> store(RunContext runContext, Iterable<Map<String, Object>> searchResponse) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
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
