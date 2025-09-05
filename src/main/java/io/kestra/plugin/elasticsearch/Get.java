package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Retrieve Elasticsearch document",
    description = "Fetches a single document by index and id. Optionally enforces a version match and can fail when the document is missing."
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
        title = "Document id"
    )
    @NotNull
    private Property<String> key;

    @Schema(
        title = "Expected version",
        description = "GET succeeds only if the document version matches this value."
    )
    private Property<Long> docVersion;

    @Schema(
        title = "Fail when missing",
        description = "If true, throws when the document is not found; default false."
    )
    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (ElasticsearchClient client = this.connection.highLevelClient(runContext)) {
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
        @Schema(
            title = "Retrieved document"
        )
        private Map<String, Object> row;
    }
}
