package io.kestra.plugin.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.logs.LogRecord;
import io.kestra.core.runners.RunContext;
import io.opentelemetry.api.common.KeyValue;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.logs.Severity;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class ElasticsearchLogShipperTest extends ElsContainer {

    public static final String LOG_INDEX = "log_index";

    @Test
    public void sendLogs() throws IOException {
        elasticsearchClient.indices().create(new CreateIndexRequest.Builder()
            .index(LOG_INDEX)
            .build());

        RunContext runContext = runContextFactory.of();
        ElasticsearchLogShipper logShipper = ElasticsearchLogShipper.builder()
            .connection(ElasticsearchConnection.builder().hosts(hosts).build())
            .chunk(Property.of(2))
            .indexName(Property.of(LOG_INDEX))
            .build();

        Flux<LogRecord> logs = Flux.fromIterable(List.of(
            LogRecord.builder()
                .timestampEpochNanos(1322907330123456789L)
                .observedTimestampEpochNanos(1322907330123456789L)
                .severity(Severity.ERROR)
                .severityText("ERROR")
                .bodyValue(buildLogEntryValue("flow1"))
                .build(),
            LogRecord.builder()
                .timestampEpochNanos(1322907330123456790L)
                .observedTimestampEpochNanos(1322907330123456790L)
                .severity(Severity.WARN)
                .severityText("WARN")
                .bodyValue(buildLogEntryValue("flow2"))
                .build(),
            LogRecord.builder()
                .timestampEpochNanos(1322907330123456791L)
                .observedTimestampEpochNanos(1322907330123456791L)
                .severity(Severity.INFO)
                .severityText("INFO")
                .bodyValue(buildLogEntryValue("flow3"))
                .build()));

        logShipper.sendLogs(runContext, logs);

        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(2D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(3D));

        elasticsearchClient.indices().refresh(RefreshRequest.of(r -> r
            .index(LOG_INDEX)
        ));
        CountResponse count = elasticsearchClient.count(CountRequest.of(c -> c
            .index(LOG_INDEX)
        ));
        assertThat(count.count(), is(3L));
    }

    private static Value<List<KeyValue>> buildLogEntryValue(String flowID){
        return Value.of(LogEntry.builder()
            .tenantId("tenantId")
            .namespace("namespace")
            .flowId(flowID)
            .taskId("taskId")
            .executionId("executionId")
            .taskRunId("taskRunId")
            .attemptNumber(1)
            .triggerId("triggerId")
            .thread("thread")
            .message("message")
            .build()
            .toMap()
            .entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> Value.of(entry.getValue()))));
    }



}
