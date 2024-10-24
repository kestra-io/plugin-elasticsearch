package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Get an ElasticSearch document."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: elasticsearch_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.elasticsearch.Get
                    connection:
                      hosts: 
                       - "http://localhost:9200"
                    index: "my_index"
                    key: "my_id"
                """
        )
    }
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "The ElasticSearch index."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String index;

    @Schema(
        title = "The ElasticSearch id."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String key;

    @Schema(
        title = "Sets the version",
        description = "which will cause the get operation to only be performed if a matching version exists and no changes happened on the doc since then."
    )
    @PluginProperty
    @NotNull
    private Long version;


    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            ElasticsearchClient client = new ElasticsearchClient(transport);
            String index = runContext.render(this.index);
            String key = runContext.render(this.key);

            var request = new GetRequest.Builder();
            request.index(index).id(key);

            if (this.version != null) {
                request.version(this.version);
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            if (this.routing != null) {
                request.routing(this.routing);
            }

            GetResponse<Map> response = client.get(request.build(), Map.class);
            logger.debug("Getting doc: {}", request);

            return Output.builder()
                .row(response.source())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private Map<String, Object> row;
    }
}
