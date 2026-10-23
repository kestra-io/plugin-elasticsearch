package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import co.elastic.clients.elasticsearch.core.bulk.*;
import co.elastic.clients.json.JsonpMappingException;
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
        Reads an Elasticsearch bulk file (NDJSON or ION) from Kestra storage and replays operations. Follows the bulk file contract; see Elasticsearch bulk API docs for format.
        Action-line metadata is carried onto each operation so the replay honors the file's concurrency and routing
        intents: routing, if_seq_no, if_primary_term, version and version_type apply to every op; pipeline and
        dynamic_templates apply to index and create; require_alias applies to index, create and update;
        retry_on_conflict applies to update.
        Update bodies that contain native update fields (doc, script, upsert, doc_as_upsert, scripted_upsert, detect_noop or _source) are applied with Elasticsearch bulk API semantics, so upserts only happen when the body asks for them.
        For compatibility, update bodies without any native update field are treated as plain partial documents with doc_as_upsert enabled.
        A plain partial document that itself has source fields named like native update fields is ambiguous: wrap it in a doc field, or use the Load task with opType UPDATE instead.
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

    // fields that mark an update body as a native Elasticsearch update request
    private static final Set<String> UPDATE_FIELDS = Set.of(
        "doc", "script", "upsert", "doc_as_upsert", "scripted_upsert", "detect_noop", "_source"
    );

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
                        var updateLine = input.readLine();
                        var updateOperation = metadata(new UpdateOperation.Builder<>(), value)
                            .retryOnConflict(OBJECT_MAPPER.convertValue(value.get("retry_on_conflict"), Integer.class))
                            .requireAlias(OBJECT_MAPPER.convertValue(value.get("require_alias"), Boolean.class))
                            .action(updateAction(OBJECT_MAPPER.convertValue(value.get("_id"), String.class), isJson, updateLine, parseline(isJson, updateLine)));
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

    // legacy: bodies without native update fields are upserted as plain documents
    static UpdateAction<Object, Object> updateAction(String id, Boolean isJson, String line, Map<?, ?> body) throws JsonProcessingException {
        if (body.keySet().stream().anyMatch(UPDATE_FIELDS::contains)) {
            try {
                return new UpdateAction.Builder<>()
                    .withJson(new StringReader(isJson ? line : OBJECT_MAPPER.writeValueAsString(body)))
                    .build();
            } catch (JsonpMappingException e) {
                throw new IllegalArgumentException(
                    "Update body for _id '" + id + "' mixes native update fields with document fields: " +
                        "wrap the document in 'doc', or use the Load task with opType UPDATE for plain records",
                    e
                );
            }
        }

        return new UpdateAction.Builder<>()
            .doc(body)
            .docAsUpsert(true)
            .build();
    }

    private static <T extends BulkOperationBase.AbstractBuilder<T>> T metadata(T builder, Map<String, Object> value) {
        return builder
            .id(OBJECT_MAPPER.convertValue(value.get("_id"), String.class))
            .index(OBJECT_MAPPER.convertValue(value.get("_index"), String.class))
            .routing(OBJECT_MAPPER.convertValue(value.get("routing"), String.class))
            .ifSeqNo(OBJECT_MAPPER.convertValue(value.get("if_seq_no"), Long.class))
            .ifPrimaryTerm(OBJECT_MAPPER.convertValue(value.get("if_primary_term"), Long.class))
            .version(OBJECT_MAPPER.convertValue(value.get("version"), Long.class))
            .versionType(versionType(value.get("version_type")));
    }

    private static <T extends WriteOperation.AbstractBuilder<T>> T writeMetadata(T builder, Map<String, Object> value) {
        metadata(builder, value)
            .pipeline(OBJECT_MAPPER.convertValue(value.get("pipeline"), String.class))
            .requireAlias(OBJECT_MAPPER.convertValue(value.get("require_alias"), Boolean.class));
        if (value.get("dynamic_templates") != null) {
            Map<String, String> dynamicTemplates = OBJECT_MAPPER.convertValue(
                value.get("dynamic_templates"),
                new TypeReference<Map<String, String>>() {}
            );
            builder.dynamicTemplates(dynamicTemplates);
        }
        return builder;
    }

    private static VersionType versionType(Object raw) {
        if (raw == null) {
            return null;
        }
        String value = OBJECT_MAPPER.convertValue(raw, String.class);
        try {
            return VersionType._DESERIALIZER.parse(value);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException(
                "Invalid version_type '" + value + "': expected one of " + allowedVersionTypes(),
                e
            );
        }
    }

    private static String allowedVersionTypes() {
        return Arrays.stream(VersionType.values()).map(VersionType::jsonValue).collect(Collectors.joining(", "));
    }

    private static Map<?, ?> parseline(Boolean isJson, String line) throws JsonProcessingException {
        if (isJson) {
            return OBJECT_MAPPER.readValue(line, JacksonMapper.MAP_TYPE_REFERENCE);
        } else {
            return JacksonMapper.ofIon().readValue(line, JacksonMapper.MAP_TYPE_REFERENCE);
        }
    }
}
