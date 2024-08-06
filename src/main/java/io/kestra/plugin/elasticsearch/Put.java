package io.kestra.plugin.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.elasticsearch.model.OpType;
import io.kestra.plugin.elasticsearch.model.RefreshPolicy;
import io.kestra.plugin.elasticsearch.model.XContentType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Put an ElasticSearch document."
)
@Plugin(
    examples = {
        @Example(
            title = "Put a document with a Map.",
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "index: \"my_index\"",
                "key: \"my_id\"",
                "value:",
                "  name: \"John Doe\"",
                "  city: \"Paris\"",
            }
        ),
        @Example(
            title = "Put a document from a JSON string.",
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "index: \"my_index\"",
                "key: \"my_id\"",
                "value: \"{{ outputs.task_id.data | json }}\""
            }
        ),
    }
)
public class Put extends AbstractTask implements RunnableTask<Put.Output> {
    private static ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "The elasticsearch index."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String index;

    @Schema(
        title = "Sets the type of operation to perform."
    )
    @PluginProperty
    private OpType opType;

    @Schema(
        title = "The elasticsearch id."
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The elasticsearch value.",
        description = "Can be a string. In this case, the contentType will be used or a raw Map."
    )
    @PluginProperty(dynamic = true)
    private Object value;

    @Schema(
        title = "Should this request trigger a refresh.",
        description = "an immediate refresh `IMMEDIATE`, wait for a refresh `WAIT_UNTIL`, or proceed ignore refreshes entirely `NONE`."
    )
    @PluginProperty
    @Builder.Default
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    @Schema(
        title = "The content type of `value`."
    )
    @PluginProperty
    @Builder.Default
    private XContentType contentType = XContentType.JSON;

    @Override
    public Put.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            OpenSearchClient client = new OpenSearchClient(transport);
            String index = runContext.render(this.index);
            String key = runContext.render(this.key);

            var request = new IndexRequest.Builder<Map>();
            request.index(index);


            this.source(runContext, request);

            if (key != null) {
                request.id(key);
            }

            if (this.opType != null) {
                request.opType(this.opType.to());
            }

            if (this.refreshPolicy != null) {
                request.refresh(this.refreshPolicy.to());
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            logger.debug("Putting doc: {}", request);

            IndexResponse response = client.index(request.build());

            return Output.builder()
                .id(response.id())
                .result(response.result())
                .version(response.version())
                .build();
        }
    }

    @SuppressWarnings("unchecked")
    private void source(RunContext runContext, IndexRequest.Builder<Map> request) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (this.value instanceof String valueStr) {
            Map<?, ?> document = MAPPER.readValue(runContext.render(valueStr), Map.class);
            // FIXME contentType
            request.document(document);
        } else if (this.value instanceof Map valueMap) {
            request.document(runContext.render(valueMap));
        } else {
            throw new IllegalVariableEvaluationException("Invalid value type '" + this.value.getClass() + "'");
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The id of the document changed."
        )
        private String id;

        @Schema(
            title = "The change that occurred to the document."
        )
        private Result result;

        @Schema(
            title = "The version of the updated document."
        )
        private Long version;
    }
}
