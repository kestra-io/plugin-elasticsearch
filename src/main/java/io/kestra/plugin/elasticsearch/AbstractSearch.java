package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.builder.SearchSourceBuilder;

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
    private Object request;

    @Schema(
        title = "The content type of `value`."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private XContentType contentType = XContentType.JSON;

    protected SearchRequest request(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        SearchRequest request = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        try (XContentParser xContentParser = ElasticsearchService.toXContentParser(runContext, this.request, this.contentType)) {
            searchSourceBuilder.parseXContent(xContentParser);
        }

        request.source(searchSourceBuilder);

        if (this.indexes != null) {
            request.indices(runContext.render(this.indexes).toArray(String[]::new));
        }

        if (this.routing != null) {
            request.routing(this.routing);
        }

        return request;
    }
}
