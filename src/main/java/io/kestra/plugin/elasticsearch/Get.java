package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    @NotNull
    private Property<String> index;

    @Schema(
        title = "The ElasticSearch id."
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Current version of the document",
        description = " The specified version must match the current version of the document for the GET request to succeed."
    )
    private Property<Long> docVersion;

    @Schema(
        title = "Raise an error if the document is not found."
    )
    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            ElasticsearchClient client = new ElasticsearchClient(transport);
            String index = runContext.render(this.index).as(String.class).orElseThrow();
            String key = runContext.render(this.key).as(String.class).orElseThrow();

            var request = new GetRequest.Builder();
            request.index(index).id(key);

            if (this.docVersion != null) {
                request.version(runContext.render(this.docVersion).as(Long.class).orElseThrow());
            }

            if (this.routing != null) {
                request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
            }

            GetResponse<Map> response = client.get(request.build(), Map.class);
            logger.debug("Getting doc: {}", request);

            if (!response.found() && runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)) {
                throw new IllegalStateException("Document with key '" + key + "' not found in index '" + index + "'");
            }

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
