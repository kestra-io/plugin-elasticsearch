package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "The source file"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The size of chunk for every bulk request"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Integer chunk = 1000;

    abstract protected Flowable<DocWriteRequest<?>> source(RunContext runContext, BufferedReader inputStream);

    @Override
    public AbstractLoad.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        try (
            RestHighLevelClient client = this.connection.client(runContext);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))
        ) {
            AtomicLong count = new AtomicLong();
            AtomicLong duration = new AtomicLong();

            Flowable<BulkResponse> flowable = this.source(runContext, inputStream)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(this.chunk, this.chunk)
                .map(indexRequests -> {
                    BulkRequest bulkRequest = new BulkRequest();
                    indexRequests.forEach(bulkRequest::add);

                    return client.bulk(bulkRequest, RequestOptions.DEFAULT);
                })
                .doOnNext(bulkItemResponse -> {
                    duration.addAndGet(bulkItemResponse.getTook().nanos());

                    if (bulkItemResponse.hasFailures()) {
                        throw new IOException("Indexer failed bulk '" + bulkItemResponse.buildFailureMessage() + "'");
                    }
                });

            // metrics & finalize
            Long requestCount = flowable.count().blockingGet();
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

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetch"
        )
        private Long size;
    }
}
