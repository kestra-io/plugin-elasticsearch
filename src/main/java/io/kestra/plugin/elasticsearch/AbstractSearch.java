package io.kestra.plugin.elasticsearch;

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
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.transport.OpenSearchTransport;

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
        title = "The content type of `value`."
    )
    @PluginProperty
    @Builder.Default
    private XContentType contentType = XContentType.JSON; //FIXME

    protected SearchRequest.Builder request(RunContext runContext, OpenSearchTransport transport) throws IllegalVariableEvaluationException, IOException {
        SearchRequest.Builder request;

        if (this.request instanceof String requestStr) {
            request = parseQuery(transport, requestStr).toBuilder();
        } else if (this.request instanceof Map requestMap) {
            String requestStr = MAPPER.writeValueAsString(requestMap);
            request = parseQuery(transport, requestStr).toBuilder();
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

    // Use the trick found here: https://forum.opensearch.org/t/how-to-create-index-using-json-file/11137
    private SearchRequest parseQuery(OpenSearchTransport transport, String query) throws IOException {
        try (Reader reader = new StringReader(query)) {
            JsonpMapper mapper = transport.jsonpMapper();
            JsonParser parser = mapper.jsonProvider().createParser(reader);
            return SearchRequest._DESERIALIZER.deserialize(parser, mapper);
        }
    }
}
