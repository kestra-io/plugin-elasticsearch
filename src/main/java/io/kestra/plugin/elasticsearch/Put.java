package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Put an Elasticsearch document"
)
@Plugin(
    examples = {
        @Example(
            title = "Put a doc with a Map",
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
            title = "Put a doc from a json string",
            code = {
                "index: \"my_index\"",
                "key: \"my_id\"",
                "value: \"{{ outputs.task_id.data | json }}\""
            }
        ),
    }
)
public class Put extends AbstractTask implements RunnableTask<Put.Output> {
    @Schema(
        title = "The elasticsearch indice"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String index;

    @Schema(
        title = "Sets the type of operation to perform."
    )
    @PluginProperty(dynamic = false)
    private DocWriteRequest.OpType opType;

    @Schema(
        title = "The elasticsearch id"
    )
    @PluginProperty(dynamic = true)
    private String key;

    @Schema(
        title = "The elasticsearch value",
        description = "Can be a string, in this case, the contentType will be used or a raw Map"
    )
    @PluginProperty(dynamic = true)
    private Object value;

    @Schema(
        title = "Should this request trigger a refresh",
        description = "an immediate refresh `IMMEDIATE`, wait for a refresh `WAIT_UNTIL`, or proceed ignore refreshes entirely `NONE`."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;

    @Schema(
        title = "The content type of `value`"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private XContentType contentType = XContentType.JSON;

    @Override
    public Put.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestHighLevelClient client = this.connection.client(runContext)) {

            String index = runContext.render(this.index);
            String key = runContext.render(this.key);

            IndexRequest request = new IndexRequest();
            request.index(index);


            this.source(runContext, request);

            if (key != null) {
                request.id(key);
            }

            if (this.opType != null) {
                request.opType(this.opType);
            }

            if (this.refreshPolicy != null) {
                request.setRefreshPolicy(this.refreshPolicy);
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            logger.debug("Putting doc: {}", request);

            IndexResponse response = client.index(request, RequestOptions.DEFAULT);

            return Output.builder()
                .id(response.getId())
                .result(response.getResult())
                .version(response.getVersion())
                .build();
        }
    }

    @SuppressWarnings("unchecked")
    private void source(RunContext runContext, IndexRequest request) throws IllegalVariableEvaluationException {
        if (this.value instanceof String) {
            request.source(runContext.render((String) this.value), contentType);
        } else if (this.value instanceof Map) {
            request.source(runContext.render((Map<String, Object>) this.value));
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
        private DocWriteResponse.Result result;

        @Schema(
            title = "The change that occurred to the document."
        )
        private Long version;
    }
}
