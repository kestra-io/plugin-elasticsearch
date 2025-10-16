package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
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
    title = "Get all documents from a search request and store it as a Kestra Internal Storage file."
)
@Plugin(
    metrics = {
        @Metric(name = "requests.count", type = Counter.TYPE, description = "Number of scroll requests sent"),
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of records returned"),
        @Metric(name = "requests.duration", type = Timer.TYPE, description = "Duration of scroll requests")
    },
    examples = {
        @Example(
            full = true,
            code = """
                id: elasticsearch_scroll
                namespace: company.team

                tasks:
                  - id: scroll
                    type: io.kestra.plugin.elasticsearch.Scroll
                    connection:
                      hosts: 
                        - "http://localhost:9200"
                    indexes:
                      - "my_index"
                    request:
                      query: 
                        term:
                          name:
                            value: 'john'
                """
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
            ElasticsearchClient client = new ElasticsearchClient(transport);
            // build request
            SearchRequest.Builder request = this.request(runContext);

            request.scroll(new Time.Builder().time("60s").build());

            logger.debug("Starting query: {}", request);

            // start scroll
            AtomicLong recordsCount = new AtomicLong();
            AtomicLong requestsCount = new AtomicLong();
            AtomicLong requestsDuration = new AtomicLong();

            String scrollId = null;

            try {
                SearchResponse<Map> searchResponse = client.search(request.build(), Map.class);
                HitsMetadata<Map> hits = searchResponse.hits();
                long took = searchResponse.took();
                scrollId = searchResponse.scrollId();

                do {
                    requestsDuration.addAndGet(took);
                    requestsCount.incrementAndGet();

                    Flux<Map> hitFlux = Flux.fromIterable(hits.hits()).map(hit -> hit.source());
                    Mono<Long> longMono = FileSerde.writeAll(output, hitFlux);

                    recordsCount.addAndGet(longMono.blockOptional().orElse(0L));

                    ScrollRequest searchScrollRequest = new ScrollRequest.Builder()
                        .scrollId(scrollId)
                        .scroll(new Time.Builder().time("60s").build())
                        .build();

                    ScrollResponse<Map> scrollResponse = client.scroll(searchScrollRequest, Map.class);
                    hits = scrollResponse.hits();
                    took = scrollResponse.took();
                } while (!hits.hits().isEmpty());
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

    private void clearScrollId(Logger logger, ElasticsearchClient client, String scrollId) {
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
