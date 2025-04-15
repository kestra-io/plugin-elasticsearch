package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.elasticsearch.model.OpType;
import io.kestra.plugin.elasticsearch.model.RefreshPolicy;
import io.kestra.plugin.elasticsearch.model.XContentType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Insert a document into an ElasticSearch cluster."
)
@Plugin(
    examples = {
        @Example(
            title = "Put a document with a Map.",
            full = true,
            code = """
                id: elasticsearch_put
                namespace: company.team

                tasks:
                  - id: put
                    type: io.kestra.plugin.elasticsearch.Put
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                    value:
                      name: "John Doe"
                      city: "Paris"
                """
        ),
        @Example(
            title = "Put a document from a JSON string.",
            full = true,
            code = """
                id: elasticsearch_put
                namespace: company.team

                inputs:
                  - id: value
                    type: JSON
                    defaults: {"name": "John Doe", "city": "Paris"}

                tasks:
                  - id: put
                    type: io.kestra.plugin.elasticsearch.Put
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                    value: "{{ inputs.value }}"
                """
        ),
    }
)
public class Put extends AbstractTask implements RunnableTask<Put.Output> {
    private static ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "The elasticsearch index."
    )
    @NotNull
    private Property<String> index;

    @Schema(
        title = "Sets the type of operation to perform."
    )
    private Property<OpType> opType;

    @Schema(
        title = "The elasticsearch id."
    )
    private Property<String> key;

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
    @Builder.Default
    private Property<RefreshPolicy> refreshPolicy = Property.of(RefreshPolicy.NONE);

    @Schema(
        title = "The content type of `value`."
    )
    @Builder.Default
    private Property<XContentType> contentType = Property.of(XContentType.JSON);

    @Override
    public Put.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            ElasticsearchClient client = new ElasticsearchClient(transport);
            String index = runContext.render(this.index).as(String.class).orElseThrow();
            String key = runContext.render(this.key).as(String.class).orElse(null);

            var request = new IndexRequest.Builder<Map>();
            request.index(index);


            this.source(runContext, request);

            if (key != null) {
                request.id(key);
            }

            if (this.opType != null) {
                request.opType(runContext.render(this.opType).as(OpType.class).orElseThrow().to());
            }

            if (this.refreshPolicy != null) {
                request.refresh(runContext.render(this.refreshPolicy).as(RefreshPolicy.class).orElseThrow().to());
            }

            if (this.routing != null) {
                request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
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
