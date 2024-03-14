package io.kestra.plugin.elasticsearch;

import com.google.common.base.Charsets;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.http.HttpMethod;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.entity.ContentType;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a generic ElasticSearch request."
)
@Plugin(
    examples = {
        @Example(
            title = "Inserting a document in an index using POST request.",
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "method: \"POST\"",
                "endpoint: \"my_index/_doc/john\"",
                "body:",
                "  name: \"john\""
            }
        ),
        @Example(
            title = "Searching for documents using GET request.",
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "method: \"GET\"",
                "endpoint: \"my_index/_search\"",
                "parameters:",
                "  q: \"name:\\\"John Doe\\\""
            }
        ),
        @Example(
            title = "Deleting document using DELETE request.",
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "method: \"DELETE\"",
                "endpoint: \"my_index/_doc/<_id>\"",
            }
        ),
    }
)
public class Request extends AbstractTask implements RunnableTask<Request.Output> {
    @Schema(
        title = "The http method to use"
    )
    @Builder.Default
    @PluginProperty(dynamic = false)
    protected HttpMethod method = HttpMethod.GET;

    @Schema(
        title = "The path of the request (without scheme, host, port, or prefix)"
    )
    @PluginProperty(dynamic = true)
    protected String endpoint;

    @Schema(
        title = "Query string parameters."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    protected Map<String, String> parameters;

    @Schema(
        title = "The full body",
        description = "Can be a json string or raw Map that will be converted to json"
    )
    @PluginProperty(dynamic = true)
    protected Object body;

    @Override
    public Request.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestHighLevelClient client = this.connection.client(runContext)) {

            org.opensearch.client.Request request = new org.opensearch.client.Request(
                method.name(),
                runContext.render(endpoint)
            );

            if (this.parameters != null) {
                this.parameters.entrySet()
                    .forEach(throwConsumer(e -> {
                        request.addParameter(runContext.render(e.getKey()), runContext.render(e.getValue()));
                    }));
            }

            if (this.body != null) {
                request.setEntity(EntityBuilder
                    .create()
                    .setContentType(ContentType.APPLICATION_JSON)
                    .setText(ElasticsearchService.toBody(runContext, this.body))
                    .build()
                );
            }

            logger.debug("Starting request: {}", request);

            Response response = client.getLowLevelClient().performRequest(request);

            response.getWarnings().forEach(logger::warn);

            String contentType = response.getHeader("content-type");
            String content = IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8);

            Output.OutputBuilder builder = Output.builder()
                .status(response.getStatusLine().getStatusCode());

            if (contentType.contains("application/json")) {
                builder.response = JacksonMapper.toMap(content);
            } else {
                builder.response = content;
            }

            return builder.build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private Integer status;
        private Object response;
    }
}
