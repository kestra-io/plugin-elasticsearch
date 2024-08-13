package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Scroll over search request.",
    description = "Get all documents from a search request and store it as Kestra Internal Storage file."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "indexes:",
                " - \"my_index\"",
                "request:",
                "  query: ",
                "    term:",
                "      name:",
                "        value: 'john'",
            }
        )
    }
)
public class Scroll extends AbstractSearch implements RunnableTask<Scroll.Output> {
    @Override
    public Scroll.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            RestClientTransport transport = this.connection.client(runContext);
            Writer output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            OpenSearchClient client = new OpenSearchClient(transport);
            // build request
            SearchRequest.Builder request = this.request(runContext, transport);

            request.scroll(new Time.Builder().time("60s").build());

            logger.debug("Starting query: {}", request);

            // start scroll
            AtomicLong recordsCount = new AtomicLong();
            AtomicLong requestsCount = new AtomicLong();
            AtomicLong requestsDuration = new AtomicLong();

            String scrollId = null;

            try {
                SearchResponse<Map> searchResponse = client.search(request.build(), Map.class);
                scrollId = searchResponse.scrollId();

                do {
                    requestsDuration.addAndGet(searchResponse.took());
                    requestsCount.incrementAndGet();

                    Flux<Map> hitFlux = Flux.fromIterable(searchResponse.hits().hits()).map(hit -> hit.source());
                    Mono<Long> longMono = FileSerde.writeAll(output, hitFlux);

                    recordsCount.addAndGet(longMono.block());

                    ScrollRequest searchScrollRequest = new ScrollRequest.Builder()
                        .scrollId(scrollId)
                        .scroll(new Time.Builder().time("60s").build())
                        .build();

                    searchResponse = client.scroll(searchScrollRequest, Map.class);
                } while (!searchResponse.hits().hits().isEmpty());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                this.clearScrollId(logger, client, scrollId);
            }

            // metrics
            runContext.metric(Counter.of("requests.count", requestsCount.get()));
            runContext.metric(Counter.of("records", recordsCount.get()));
            runContext.metric(Timer.of("requests.duration", Duration.ofNanos(requestsDuration.get())));

            // outputs
            return Output.builder()
                .size(recordsCount.get())
                .uri(runContext.storage().putFile(tempFile))
                .build();
        }
    }

    private void clearScrollId(Logger logger, OpenSearchClient client, String scrollId) {
        if (scrollId == null) {
            return;
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest.Builder()
            .scrollId(scrollId)
            .build();

        try {
            client.clearScroll(clearScrollRequest);
        } catch (IOException e) {
            logger.warn("Failed to clear scroll", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The size of the rows fetch"
        )
        private Long size;

        @Schema(
            title = "The uri of store result"
        )
        private URI uri;
    }
}
