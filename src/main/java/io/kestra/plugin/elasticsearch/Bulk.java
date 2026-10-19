package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationBase;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.elasticsearch.core.bulk.WriteOperation;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Replay Elasticsearch bulk file",
    description = """
        Reads an Elasticsearch bulk file (NDJSON or ION) from Kestra storage and replays operations with their action metadata.
        Update bodies containing `doc`, `script`, `upsert`, `doc_as_upsert`, `scripted_upsert`, `detect_noop` or `_source` follow the Elasticsearch bulk API, without enabling upsert by default.
        For compatibility, update bodies without any of these keys are treated as plain partial documents with `doc_as_upsert: true`.
        Wrap source fields with these reserved names inside `doc` or use `Load` with `opType: UPDATE` for plain records.
        """
)
@Plugin(
    metrics = {
        @Metric(name = "requests.count", type = Counter.TYPE, description = "Number of bulk requests sent"),
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of records loaded"),
        @Metric(name = "requests.duration", type = Timer.TYPE, description = "Duration of bulk requests")
    },
    examples = {
        @Example(
            full = true,
            code = """
                id: elasticsearch_bulk_load
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: bulk_load
                    type: io.kestra.plugin.elasticsearch.Bulk
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    from: "{{ inputs.file }}"
                """
        )
    }
)
public class Bulk extends AbstractLoad implements RunnableTask<Bulk.Output> {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();
    private static final Set<String> UPDATE_FIELDS = Set.of("doc", "script", "upsert", "doc_as_upsert", "scripted_upsert", "detect_noop", "_source");

    @Override
    protected Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IOException {
        return Flux
            .create(this.fileReader(new BufferedReader(new InputStreamReader(inputStream))), FluxSink.OverflowStrategy.BUFFER);
    }

    @SuppressWarnings("unchecked")
    public Consumer<FluxSink<BulkOperation>> fileReader(BufferedReader input) throws IOException {
        return throwConsumer(s ->
        {
            String row;
            Boolean isJson = null;

            while ((row = input.readLine()) != null) {
                // validate if it's json or ion
                if (isJson == null) {
                    try {
                        OBJECT_MAPPER.readTree(row);
                        isJson = true;
                    } catch (JacksonException e) {
                        isJson = false;
                    }
                }

                Map<String, Object> data;
                if (isJson) {
                    data = JacksonMapper.toMap(row);
                } else {
                    data = JacksonMapper.ofIon().readValue(row, JacksonMapper.MAP_TYPE_REFERENCE);
                }

                Map.Entry<String, Object> operation = data.entrySet().iterator().next();
                Map<String, Object> value = (Map<String, Object>) operation.getValue();

                var bulkOperation = new BulkOperation.Builder();

                switch (operation.getKey()) {
                    case "index":
                        var indexOperation = writeMetadata(new IndexOperation.Builder<>(), value)
                            .document(parseline(isJson, input.readLine()));
                        bulkOperation.index(indexOperation.build());
                        break;
                    case "create":
                        var createOperation = writeMetadata(new CreateOperation.Builder<>(), value)
                            .document(parseline(isJson, input.readLine()));
                        bulkOperation.create(createOperation.build());
                        break;
                    case "update":
                        var updateOperation = metadata(new UpdateOperation.Builder<>(), value)
                            .retryOnConflict(OBJECT_MAPPER.convertValue(value.get("retry_on_conflict"), Integer.class))
                            .requireAlias((Boolean) value.get("require_alias"))
                            .action(updateAction(parseline(isJson, input.readLine())));
                        bulkOperation.update(updateOperation.build());
                        break;
                    case "delete":
                        var deleteOperation = metadata(new DeleteOperation.Builder(), value);
                        bulkOperation.delete(deleteOperation.build());
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid bulk request type on '" + row + "'");
                }

                s.next(bulkOperation.build());
            }

            s.complete();
        });
    }

    private static <T extends BulkOperationBase.AbstractBuilder<T>> T metadata(T builder, Map<String, Object> value) {
        return builder
            .id((String) value.get("_id"))
            .index((String) value.get("_index"))
            .routing((String) value.get("routing"))
            .ifSeqNo(OBJECT_MAPPER.convertValue(value.get("if_seq_no"), Long.class))
            .ifPrimaryTerm(OBJECT_MAPPER.convertValue(value.get("if_primary_term"), Long.class))
            .version(OBJECT_MAPPER.convertValue(value.get("version"), Long.class))
            .versionType(value.get("version_type") == null ? null : VersionType._DESERIALIZER.parse((String) value.get("version_type")));
    }

    @SuppressWarnings("unchecked")
    private static <T extends WriteOperation.AbstractBuilder<T>> T writeMetadata(T builder, Map<String, Object> value) {
        metadata(builder, value)
            .pipeline((String) value.get("pipeline"))
            .requireAlias((Boolean) value.get("require_alias"));
        if (value.get("dynamic_templates") != null) {
            builder.dynamicTemplates((Map<String, String>) value.get("dynamic_templates"));
        }
        return builder;
    }

    private static UpdateAction<Object, Object> updateAction(Map<?, ?> body) throws JsonProcessingException {
        if (body.keySet().stream().anyMatch(UPDATE_FIELDS::contains)) {
            return new UpdateAction.Builder<>()
                .withJson(new StringReader(OBJECT_MAPPER.writeValueAsString(body)))
                .build();
        }
        return new UpdateAction.Builder<>()
            .doc(body)
            .docAsUpsert(true)
            .build();
    }

    private static Map<?, ?> parseline(Boolean isJson, String line) throws JsonProcessingException {
        if (isJson) {
            return OBJECT_MAPPER.readValue(line, JacksonMapper.MAP_TYPE_REFERENCE);
        } else {
            return JacksonMapper.ofIon().readValue(line, JacksonMapper.MAP_TYPE_REFERENCE);
        }
    }
}
