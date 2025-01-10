package io.kestra.plugin.elasticsearch;

import static io.kestra.plugin.elasticsearch.AbstractLoad.executeBulk;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.models.tasks.logs.LogRecord;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Ship logs to Elasticsearch",
    description = """
        This task is designed to send logs from kestra to an Elasticsearch.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Ship logs to Elasticsearch",
            code = """
                id: logSync
                namespace: company.team

                triggers:
                  - id: daily
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "@daily"

                tasks:
                  - id: logSync
                    type: io.kestra.plugin.ee.core.log.LogSync
                    logLevelFilter: INFO
                    batchSize: 2
                    startingDurationBefore: P1D
                    stateName: LogSync-state
                    logShippers:
                      - type: io.kestra.plugin.elasticsearch.LogShipper
                        id: ElasticsearchLogShipper
                """,
            full = true
        )
    }
)
public class LogShipper extends io.kestra.core.models.tasks.logs.LogShipper<VoidOutput> {

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
    public VoidOutput sendLogs(RunContext runContext, Flux<LogRecord> logRecord) {
        try (
            RestClientTransport transport = this.connection.client(runContext);
        ) {
            String index = runContext.render(indexName).as(String.class).orElseThrow();
            Integer bufferSize = runContext.render(this.chunk).as(Integer.class).orElseThrow();

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
        return null;
    }
}
