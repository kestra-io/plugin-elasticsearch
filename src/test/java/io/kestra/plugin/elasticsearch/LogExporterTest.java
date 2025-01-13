package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.logs.LogRecord;
import io.kestra.core.runners.RunContext;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

public class LogExporterTest extends ElsContainer {

    public static final String LOG_INDEX = "log_index";

    @Test
    public void sendLogs() throws IOException {
        elasticsearchClient.indices().create(new CreateIndexRequest.Builder()
            .index(LOG_INDEX)
            .build());

        RunContext runContext = runContextFactory.of();
        LogExporter logExporter = LogExporter.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .chunk(Property.of(2))
            .indexName(Property.of(LOG_INDEX))
            .build();

        LogRecord logRecord1 = buildLogRecord("flow1", Instant.parse("2011-12-03T10:15:30.123456789Z"));
        LogRecord logRecord2 = buildLogRecord("flow2", Instant.parse("2011-12-03T10:15:31.123456789Z"));
        LogRecord logRecord3 = buildLogRecord("flow3", Instant.parse("2011-12-03T10:15:32.123456789Z"));

        Flux<LogRecord> logs = Flux.fromIterable(List.of(
            logRecord1,
            logRecord2,
            logRecord3));

        logExporter.sendLogs(runContext, logs);

        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(2D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(3D));

        elasticsearchClient.indices().refresh(RefreshRequest.of(r -> r
            .index(LOG_INDEX)
        ));
        CountResponse count = elasticsearchClient.count(CountRequest.of(c -> c
            .index(LOG_INDEX)
        ));
        assertThat(count.count(), is(3L));

        GetResponse<LogRecord> response = elasticsearchClient.get(
            GetRequest.of(g -> g.index(LOG_INDEX)
                .id(String.valueOf(logRecord1.getTimestampEpochNanos()))), LogRecord.class);
        assertThat(response.source().getResource(), is(logRecord1.getResource()));
        assertThat(response.source().getAttributes(), is(logRecord1.getAttributes()));
        assertThat(response.source().getTimestampEpochNanos(), is(logRecord1.getTimestampEpochNanos()));
        assertThat(response.source().getSeverity(), is(logRecord1.getSeverity()));
        assertThat(response.source().getBodyValue(), is(logRecord1.getBodyValue()));
    }

    public static LogRecord buildLogRecord(String flowId, Instant instant) {
        return LogRecord.builder()
            .resource("Kestra")
            .timestampEpochNanos(instantInNanos(instant))
            .severity(Level.INFO.name())
            .attributes(Map.of(
                "tenantId", "tenantId",
                "namespace", "namespace",
                "flowId", flowId,
                "taskId", "taskId",
                "executionId", "executionId",
                "taskRunId", "taskRunId",
                "attemptNumber", "attemptNumber",
                "triggerId", "triggerId",
                "thread", "thread",
                "message", "message"))
            .bodyValue("Body Value")
            .build();
    }

    public static long instantInNanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

}
