package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
    @NotNull
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

        request = QueryService.request(runContext, this.request);

        if (this.indexes != null) {
            request.index(runContext.render(this.indexes));
        }

        if (this.routing != null) {
            request.routing(this.routing);
        }

        return request;
    }

}
