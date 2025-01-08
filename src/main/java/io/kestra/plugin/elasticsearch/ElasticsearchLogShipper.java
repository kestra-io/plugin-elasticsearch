package io.kestra.plugin.elasticsearch;

import static io.kestra.plugin.elasticsearch.AbstractLoad.executeBulk;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.logs.LogRecord;
import io.kestra.core.models.tasks.logs.LogShipper;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import reactor.core.publisher.Flux;

@Builder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ElasticsearchLogShipper extends LogShipper {

    @Schema(
        title = "The connection properties."
    )
    @NotNull
    protected ElasticsearchConnection connection;

    @Schema(
        title = "The name of the index to send logs to"
    )
    @NotNull
    private Property<String> indexName;

    @Schema(
        title = "The chunk size for every bulk request."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Property<Integer> chunk = Property.of(1000);

    @Override
    public void sendLogs(RunContext runContext, Flux<LogRecord> logRecord) {
        try (
            RestClientTransport transport = this.connection.client(runContext);
        ) {
            String index = runContext.render(indexName).as(String.class).orElseThrow();
            Integer bufferSize = runContext.render(this.chunk).as(Integer.class).orElse(1000);
            BulkOperation.Builder bulkOperation = new BulkOperation.Builder();

            Flux<BulkOperation> operationFlux = logRecord.map(log ->
                BulkOperation.of(builder -> builder.index(
                indexBuilder -> indexBuilder.id(String.valueOf(log.getTimestampEpochNanos()))
                    .index(index)
                    .document(log)
            )));

            executeBulk(runContext, transport, operationFlux, bufferSize);
        } catch (IllegalVariableEvaluationException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
