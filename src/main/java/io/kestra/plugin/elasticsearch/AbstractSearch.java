package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.elasticsearch.model.XContentType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSearch extends AbstractTask {

    @Schema(
        title = "Target indices",
        description = "List of Elasticsearch indices to query; empty means all indices."
    )
    private Property<List<String>> indexes;

    @Schema(
        title = "Search request body",
        description = "Elasticsearch search body as Map or JSON string; rendered before execution."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object request;

    @Schema(
        title = "Request content type",
        description = "Deprecated; no longer used."
    )
    @PluginProperty
    @Builder.Default
    @Deprecated
    private XContentType contentType = XContentType.JSON;

    protected SearchRequest.Builder request(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        SearchRequest.Builder request;

        request = QueryService.request(runContext, this.request);

        if (!runContext.render(this.indexes).asList(String.class).isEmpty()) {
            request.index(runContext.render(this.indexes).asList(String.class));
        }

        if (this.routing != null) {
            request.routing(runContext.render(this.routing).as(String.class).orElseThrow());
        }

        return request;
    }

}
