package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "The source file."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The chunk size for every bulk request."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Integer chunk = 1000;

    abstract protected Flux<BulkOperation> source(RunContext runContext, BufferedReader inputStream) throws IllegalVariableEvaluationException, IOException;

    @Override
    public AbstractLoad.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        try (
            RestClientTransport transport = this.connection.client(runContext);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            OpenSearchClient client = new OpenSearchClient(transport);
            AtomicLong count = new AtomicLong();
            AtomicLong duration = new AtomicLong();

            Flux<BulkResponse> flowable = this.source(runContext, inputStream)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(this.chunk, this.chunk)
                .map(throwFunction(indexRequests -> {
                    var bulkRequest = new BulkRequest.Builder();
                    bulkRequest.operations(indexRequests);

                    return client.bulk(bulkRequest.build());
                }))
                .doOnNext(bulkItemResponse -> {
                    duration.addAndGet(bulkItemResponse.took());

                    if (bulkItemResponse.errors()) {
                        throw new RuntimeException("Indexer failed bulk:\n " + logError(bulkItemResponse));
                    }
                });

            // metrics & finalize
            Long requestCount = flowable.count().block();
            runContext.metric(Counter.of("requests.count", requestCount));
            runContext.metric(Counter.of("records", count.get()));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(duration.get())));

            logger.info(
                "Successfully send {} requests for {} records in {}",
                requestCount,
                count.get(),
                Duration.ofNanos(duration.get())
            );

            return Output.builder()
                .size(count.get())
                .build();
        }
    }

    private String logError(BulkResponse bulkResponse) {
        StringBuilder builder = new StringBuilder();
        bulkResponse.items().forEach(
            responseItem -> {
                if (responseItem.error() != null) {
                    builder
                        .append(responseItem.index()).append(": ")
                        .append(responseItem.status()).append(" - ")
                        .append(responseItem.error().reason()).append('\n');
                }
            }
        );
        return builder.toString();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetched."
        )
        private Long size;
    }
}
