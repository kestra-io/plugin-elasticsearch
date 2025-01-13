package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
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
    @NotNull
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The chunk size for every bulk request."
    )
    @Default
    private Property<Integer> chunk = Property.of(1000);

    abstract protected Flux<BulkOperation> source(RunContext runContext, BufferedReader inputStream) throws IllegalVariableEvaluationException, IOException;

    @Override
    public AbstractLoad.Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from));

        try (
                RestClientTransport transport = this.connection.client(runContext);
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            Integer bufferSize = runContext.render(this.chunk).as(Integer.class).orElseThrow();
            Flux<BulkOperation> operationFlux = this.source(runContext, inputStream);

            AtomicLong count = executeBulk(runContext, transport, operationFlux, bufferSize);

            return Output.builder()
                .size(count.get())
                .build();
        }
    }

    public static AtomicLong executeBulk(RunContext runContext, RestClientTransport transport,
        Flux<BulkOperation> operationFlux, Integer bufferSize) throws IOException {
        ElasticsearchClient client = new ElasticsearchClient(transport);
        AtomicLong count = new AtomicLong();
        AtomicLong duration = new AtomicLong();
        Logger logger = runContext.logger();

        Flux<BulkResponse> flowable = operationFlux
            .doOnNext(docWriteRequest -> {
                count.incrementAndGet();
            })
            .buffer(bufferSize, bufferSize)
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
        Long requestCount = flowable.count().blockOptional().orElse(0L);
        runContext.metric(Counter.of("requests.count", requestCount));
        runContext.metric(Counter.of("records", count.get()));
        runContext.metric(Timer.of("requests.duration", Duration.ofNanos(duration.get())));

        logger.info(
            "Successfully send {} requests for {} records in {}",
            requestCount,
            count.get(),
            Duration.ofNanos(duration.get())
        );
        return count;
    }

    private static String logError(BulkResponse bulkResponse) {
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
