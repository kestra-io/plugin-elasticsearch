package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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
        ION files can be text or binary, so files written by upstream tasks are read on every Kestra version.
        Action-line metadata is carried onto each operation so the replay honors the file's concurrency and routing
        intents: routing, if_seq_no, if_primary_term, version and version_type apply to every op; pipeline and
        dynamic_templates apply to index and create; require_alias applies to index, create and update;
        retry_on_conflict applies to update.
        Update bodies that contain native update fields (doc, script, upsert, doc_as_upsert, scripted_upsert, detect_noop or _source) are applied with Elasticsearch bulk API semantics, so upserts only happen when the body asks for them.
        For compatibility, update bodies without any native update field are treated as plain partial documents with doc_as_upsert enabled, unless the action line sets if_seq_no, which Elasticsearch does not allow with upserts.
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

    private static final JavaType DYNAMIC_TEMPLATES_TYPE = OBJECT_MAPPER.getTypeFactory()
        .constructMapType(Map.class, String.class, String.class);

    @Override
    protected Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IOException {
        return this.operations(inputStream);
    }

    Flux<BulkOperation> operations(InputStream input) throws IOException {
        var reader = isNdjson(input)
            ? this.fileReader(new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8)))
            : this.ionReader(input);

        return Flux.create(reader, FluxSink.OverflowStrategy.BUFFER);
    }

    public Consumer<FluxSink<BulkOperation>> fileReader(BufferedReader input) throws IOException {
        return this.reader(() ->
        {
            var line = input.readLine();
            return line == null ? null : new Row(JacksonMapper.toMap(line), line);
        });
    }

    // intentionally mirrors FileSerde.readAll on Kestra 2.x: FileSerde on the Kestra 1.x build only reads text Ion, while this reader detects text and binary
    private Consumer<FluxSink<BulkOperation>> ionReader(InputStream input) throws IOException {
        MappingIterator<Map<String, Object>> records = JacksonMapper.ofIon()
            .readerFor(JacksonMapper.MAP_TYPE_REFERENCE)
            .readValues(input);

        return this.reader(() -> records.hasNext() ? new Row(records.next(), null) : null);
    }

    // peeks at the first line without consuming it: only a JSON first line keeps the line-based NDJSON reader
    private static boolean isNdjson(InputStream input) throws IOException {
        input.mark(FileSerde.BUFFER_SIZE);
        try {
            var firstLine = new ByteArrayOutputStream();
            int b;
            while (firstLine.size() < FileSerde.BUFFER_SIZE && (b = input.read()) != -1 && b != '\n') {
                firstLine.write(b);
            }
            OBJECT_MAPPER.readTree(firstLine.toString(StandardCharsets.UTF_8));
            return true;
        } catch (JacksonException e) {
            return false;
        } finally {
            input.reset();
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<FluxSink<BulkOperation>> reader(RowSource rows) throws IOException {
        return throwConsumer(s ->
        {
            Row row;

            while ((row = rows.next()) != null) {
                Map.Entry<String, Object> operation = row.data().entrySet().iterator().next();
                Map<String, Object> action = (Map<String, Object>) operation.getValue();

                var bulkOperation = new BulkOperation.Builder();

                switch (operation.getKey()) {
                    case "index":
                        var indexOperation = writeMetadata(new IndexOperation.Builder<>(), action)
                            .document(body(rows, operation.getKey(), action).data());
                        bulkOperation.index(indexOperation.build());
                        break;
                    case "create":
                        var createOperation = writeMetadata(new CreateOperation.Builder<>(), action)
                            .document(body(rows, operation.getKey(), action).data());
                        bulkOperation.create(createOperation.build());
                        break;
                    case "update":
                        var updateOperation = metadata(new UpdateOperation.Builder<>(), action)
                            .retryOnConflict(field(action, "retry_on_conflict", Integer.class))
                            .requireAlias(field(action, "require_alias", Boolean.class))
                            .action(updateAction(action, body(rows, operation.getKey(), action)));
                        bulkOperation.update(updateOperation.build());
                        break;
                    case "delete":
                        var deleteOperation = metadata(new DeleteOperation.Builder(), action);
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

    private static Row body(RowSource rows, String type, Map<String, Object> action) throws IOException {
        var body = rows.next();
        if (body == null) {
            var target = action.get("_id") != null ? "_id '" + action.get("_id") + "'" : "index '" + action.get("_index") + "'";
            throw new IllegalArgumentException(
                "Bulk file ends after the '" + type + "' action for " + target + ": " +
                    "index, create and update actions must be followed by a body line"
            );
        }
        return body;
    }

    // legacy: bodies without native update fields are upserted as plain documents, except conditional writes since ES rejects if_seq_no with upsert
    static UpdateAction<Object, Object> updateAction(Map<String, Object> action, Row row) throws JsonProcessingException {
        var body = row.data();
        if (body.keySet().stream().anyMatch(UPDATE_FIELDS::contains)) {
            try {
                return new UpdateAction.Builder<>()
                    .withJson(new StringReader(row.json() != null ? row.json() : OBJECT_MAPPER.writeValueAsString(body)))
                    .build();
            } catch (JsonpMappingException e) {
                throw new IllegalArgumentException(
                    "Update body for _id '" + action.get("_id") + "' mixes native update fields with document fields: " +
                        "wrap the document in 'doc', or use the Load task with opType UPDATE for plain records",
                    e
                );
            }
        }

        return new UpdateAction.Builder<>()
            .doc(body)
            .docAsUpsert(action.get("if_seq_no") == null ? true : null)
            .build();
    }

    private static <T extends BulkOperationBase.AbstractBuilder<T>> T metadata(T builder, Map<String, Object> action) {
        return builder
            .id(field(action, "_id", String.class))
            .index(field(action, "_index", String.class))
            .routing(field(action, "routing", String.class))
            .ifSeqNo(field(action, "if_seq_no", Long.class))
            .ifPrimaryTerm(field(action, "if_primary_term", Long.class))
            .version(field(action, "version", Long.class))
            .versionType(versionType(action));
    }

    private static <T extends WriteOperation.AbstractBuilder<T>> T writeMetadata(T builder, Map<String, Object> action) {
        metadata(builder, action)
            .pipeline(field(action, "pipeline", String.class))
            .requireAlias(field(action, "require_alias", Boolean.class));
        Map<String, String> dynamicTemplates = field(action, "dynamic_templates", DYNAMIC_TEMPLATES_TYPE);
        if (dynamicTemplates != null) {
            builder.dynamicTemplates(dynamicTemplates);
        }
        return builder;
    }

    private static VersionType versionType(Map<String, Object> action) {
        var raw = field(action, "version_type", String.class);
        if (raw == null) {
            return null;
        }
        try {
            return VersionType._DESERIALIZER.parse(raw);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException(
                "Invalid version_type '" + raw + "': expected one of " +
                    Arrays.stream(VersionType.values()).map(VersionType::jsonValue).collect(Collectors.joining(", ")),
                e
            );
        }
    }

    private static <T> T field(Map<String, Object> action, String key, Class<T> type) {
        return field(action, key, OBJECT_MAPPER.constructType(type));
    }

    private static <T> T field(Map<String, Object> action, String key, JavaType type) {
        try {
            return OBJECT_MAPPER.convertValue(action.get(key), type);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid '" + key + "' in bulk action line for _id '" + action.get("_id") + "': expected " +
                    type.getRawClass().getSimpleName() + ", got '" + action.get(key) + "'",
                e
            );
        }
    }

    // json is the raw NDJSON line, kept so native update bodies are passed through unchanged; null for Ion records
    record Row(Map<String, Object> data, String json) {
        @Override
        public String toString() {
            return json != null ? json : String.valueOf(data);
        }
    }

    @FunctionalInterface
    interface RowSource {
        Row next() throws IOException;
    }
}
