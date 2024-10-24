package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.elasticsearch.model.XContentType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.json.stream.JsonParser;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSearch extends AbstractTask {
    private static ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "The ElasticSearch indices.",
        description = "Default to all indices."
    )
    @PluginProperty(dynamic = true)
    private List<String> indexes;

    @Schema(
        title = "The ElasticSearch value.",
        description = "Can be a JSON string. In this case, the contentType will be used or a raw Map."
    )
    @PluginProperty(dynamic = true)
    private Object request;

    @Schema(
        title = "The content type of `value`.",
        description = "Not used anymore."
    )
    @PluginProperty
    @Builder.Default
    @Deprecated
    private XContentType contentType = XContentType.JSON;

    protected SearchRequest.Builder request(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        SearchRequest.Builder request;

        if (this.request instanceof String requestStr) {
            request = parseQuery(requestStr);
        } else if (this.request instanceof Map requestMap) {
            String requestStr = MAPPER.writeValueAsString(requestMap);
            request = parseQuery(requestStr);
        } else {
            throw new IllegalArgumentException("The `request` property must be a String or an Object");
        }

        if (this.indexes != null) {
            request.index(runContext.render(this.indexes));
        }

        if (this.routing != null) {
            request.routing(this.routing);
        }

        return request;
    }

    private SearchRequest.Builder parseQuery(String query) throws IOException {
        try (Reader reader = new StringReader(query)) {
            return new SearchRequest.Builder().withJson(reader);
        }
    }
}
