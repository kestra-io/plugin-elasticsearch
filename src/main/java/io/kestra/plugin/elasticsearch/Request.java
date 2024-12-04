package io.kestra.plugin.elasticsearch;

import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.common.base.Charsets;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.elasticsearch.model.HttpMethod;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Response;
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
            full = true,
            code = """
                id: elasticsearch_request
                namespace: company.team

                tasks:
                  - id: request_post
                    type: io.kestra.plugin.elasticsearch.Request
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    method: "POST"
                    endpoint: "my_index/_doc/john"
                    body:
                      name: "john"
                """
        ),
        @Example(
            title = "Searching for documents using GET request.",
            full = true,
            code = """
                id: elasticsearch_request
                namespace: company.team

                tasks:
                  - id: request_get
                    type: io.kestra.plugin.elasticsearch.Request
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    method: "GET"
                    endpoint: "my_index/_search"
                    parameters:
                      q: "name:\"John Doe\""
                """
        ),
        @Example(
            title = "Deleting document using DELETE request.",
            full = true,
            code = """
                id: elasticsearch_request
                namespace: company.team

                tasks:
                  - id: request_delete
                    type: io.kestra.plugin.elasticsearch.Request
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    method: "DELETE"
                    endpoint: "my_index/_doc/<_id>"
                """
        ),
    }
)
public class Request extends AbstractTask implements RunnableTask<Request.Output> {
    @Schema(
        title = "The http method to use."
    )
    @Builder.Default
    protected Property<HttpMethod> method = Property.of(HttpMethod.GET);

    @Schema(
        title = "The path of the request (without scheme, host, port, or prefix)."
    )
    @NotNull
    protected Property<String> endpoint;

    @Schema(
        title = "Query string parameters."
    )
    protected Property<Map<String, String>> parameters;

    @Schema(
        title = "The full body.",
        description = "Can be a JSON string or raw Map that will be converted to json."
    )
    @PluginProperty(dynamic = true)
    protected Object body;

    @Override
    public Request.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        try (RestClientTransport transport = this.connection.client(runContext)) {
            org.elasticsearch.client.Request request = new org.elasticsearch.client.Request(
                runContext.render(method).as(HttpMethod.class).orElseThrow().name(),
                runContext.render(endpoint).as(String.class).orElseThrow()
            );

            if (this.parameters != null) {
                runContext.render(this.parameters).asMap(String.class, String.class)
                    .entrySet()
                    .forEach(throwConsumer(e -> {
                        request.addParameter(e.getKey(), e.getValue());
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

            Response response = transport.restClient().performRequest(request);

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
