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
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Scroll over search request",
    description = "Get all documents from a search request and store it as Kestra Internal Storage file"
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
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            RestHighLevelClient client = this.connection.client(runContext);
            OutputStream output = new FileOutputStream(tempFile)
        ) {
            // build request
            SearchRequest request = this.request(runContext);

            request.scroll(new TimeValue(60000));

            logger.debug("Starting query: {}", request);

            // start scroll
            AtomicLong recordsCount = new AtomicLong();
            AtomicLong requestsCount = new AtomicLong();
            AtomicLong requestsDuration = new AtomicLong();

            String scrollId = null;

            try {
                SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();

                do {
                    requestsDuration.addAndGet(searchResponse.getTook().nanos());
                    requestsCount.incrementAndGet();

                    Arrays.stream(searchResponse.getHits().getHits())
                        .forEach(throwConsumer(documentFields -> {
                            recordsCount.incrementAndGet();
                            FileSerde.write(output, documentFields.getSourceAsMap());
                        }));

                    SearchScrollRequest searchScrollRequest = new SearchScrollRequest()
                        .scrollId(scrollId)
                        .scroll(new TimeValue(60000));

                    searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                } while (searchResponse.getHits().getHits().length != 0);
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
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }

    private void clearScrollId(Logger logger, RestHighLevelClient client, String scrollId) {
        if (scrollId == null) {
            return;
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);

        try {
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
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
