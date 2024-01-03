package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get an Elasticsearch document"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "index: \"my_index\"",
                "key: \"my_id\"",
            }
        )
    }
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "The elasticsearch indice"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String index;

    @Schema(
        title = "The elasticsearch id"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String key;

    @Schema(
        title = "Sets the version",
        description = "which will cause the get operation to only be performed if a matching version exists and no changes happened on the doc since then."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    private Long version;


    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestHighLevelClient client = this.connection.client(runContext)) {

            String index = runContext.render(this.index);
            String key = runContext.render(this.key);

            GetRequest request = new GetRequest(index, key);

            if (this.version != null) {
                request.version(this.version);
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            logger.debug("Getting doc: {}", request);

            return Output.builder()
                .row(response.getSourceAsMap())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private Map<String, Object> row;
    }
}
