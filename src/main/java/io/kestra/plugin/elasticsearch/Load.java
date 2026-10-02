package io.kestra.plugin.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.elasticsearch.model.OpType;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load from Kestra storage",
    description = "Reads ION-serialized records from a Kestra internal storage file and indexes them in bulk. Uses the parent chunk size; set `removeIdKey` to keep or drop the id field after use."
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
                id: elasticsearch_load
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: load
                    type: io.kestra.plugin.elasticsearch.Load
                    connection:
                      hosts:
                       - "http://localhost:9200"
                    from: "{{ inputs.file }}"
                    index: "my_index"
                """
        )
    }
)
public class Load extends AbstractLoad implements RunnableTask<Load.Output> {

    @Schema(
        title = "The Elasticsearch index"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> index;

    @Schema(
        title = "Operation type",
        description = """
            Bulk operation applied to each record: `INDEX` (default), `CREATE`, `UPDATE` (partial update, upserting the record if missing), or `DELETE`.
            `CREATE` fails the task if a document with the same id already exists.
            `UPDATE` and `DELETE` require `idKey`."""
    )
    @PluginProperty(group = "advanced")
    private Property<OpType> opType;

    @Schema(
        title = "Field used as document id",
        description = "Name of the field to use as `_id`; required when assigning ids from input rows."
    )
    @PluginProperty(group = "connection")
    private Property<String> idKey;

    @Schema(
        title = "Remove id field from document",
        description = "When true (default), drops the idKey field from the indexed document body."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> removeIdKey = Property.ofValue(true);

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IllegalVariableEvaluationException, IOException {
        var index = runContext.render(this.index).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`index` is required"));
        var opType = runContext.render(this.opType).as(OpType.class).orElse(OpType.INDEX);
        var idKey = runContext.render(this.idKey).as(String.class).orElse(null);
        var removeIdKey = runContext.render(this.removeIdKey).as(Boolean.class).orElse(true);

        if (idKey == null && (opType == OpType.UPDATE || opType == OpType.DELETE)) {
            throw new IllegalArgumentException("`idKey` is required when `opType` is " + opType);
        }

        return FileSerde.readAll(inputStream)
            .map(o ->
            {
                var values = (Map<String, ?>) o;

                String id = null;
                if (idKey != null) {
                    var idValue = values.get(idKey);
                    if (idValue == null) {
                        throw new IllegalArgumentException("Record is missing idKey '" + idKey + "'; required for opType " + opType);
                    }
                    id = idValue.toString();

                    if (removeIdKey) {
                        values.remove(idKey);
                    }
                }

                return operation(opType, index, id, values);
            });
    }

    private static BulkOperation operation(OpType opType, String index, String id, Map<String, ?> values) {
        var bulkOperation = new BulkOperation.Builder();

        switch (opType) {
            case INDEX -> bulkOperation.index(
                new IndexOperation.Builder<Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .document(values)
                    .build()
            );
            case CREATE -> bulkOperation.create(
                new CreateOperation.Builder<Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .document(values)
                    .build()
            );
            case UPDATE -> bulkOperation.update(
                new UpdateOperation.Builder<Map<String, ?>, Map<String, ?>>()
                    .index(index)
                    .id(id)
                    .action(
                        new UpdateAction.Builder<Map<String, ?>, Map<String, ?>>()
                            .docAsUpsert(true)
                            .doc(values)
                            .build()
                    )
                    .build()
            );
            case DELETE -> bulkOperation.delete(
                new DeleteOperation.Builder()
                    .index(index)
                    .id(id)
                    .build()
            );
        }

        return bulkOperation.build();
    }
}
