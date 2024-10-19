package io.kestra.plugin.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load documents in ElasticSearch using [bulk files](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) elastic files."
)
@Plugin(
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

    @Override
    protected Flux<BulkOperation> source(RunContext runContext, BufferedReader inputStream) throws IOException {
        return Flux
            .create(this.esNdJSonReader(inputStream), FluxSink.OverflowStrategy.BUFFER);
    }

    @SuppressWarnings("unchecked")
    public Consumer<FluxSink<BulkOperation>> esNdJSonReader(BufferedReader input) throws IOException {
        return throwConsumer(s -> {
            String row;

            while ((row = input.readLine()) != null) {
                Map.Entry<String, Object> operation = JacksonMapper.toMap(row).entrySet().iterator().next();
                Map<String, Object> value = (Map<String, Object>) operation.getValue();

                var bulkOperation = new BulkOperation.Builder();

                switch (operation.getKey()) {
                    case "index":
                        var indexOperation = new IndexOperation.Builder<>()
                            .id((String) value.get("_id"))
                            .index((String) value.get("_index"))
                            .document(parseline(input.readLine()));
                        bulkOperation.index(indexOperation.build());
                        break;
                    case "create":
                        var createOperation = new CreateOperation.Builder<>()
                            .id((String) value.get("_id"))
                            .index((String) value.get("_index"))
                            .ifPrimaryTerm(0L) //FIXME opType
                            .document(parseline(input.readLine()));
                        bulkOperation.create(createOperation.build());
                        break;
                    case "update":
                        var updateOperation = new UpdateOperation.Builder<>()
                            .id((String) value.get("_id"))
                            .index((String) value.get("_index"))
                            .docAsUpsert(true)
                            .document(parseline(input.readLine()));
                        bulkOperation.update(updateOperation.build());
                        break;
                    case "delete":
                        var deleteOperation = new DeleteOperation.Builder()
                            .id((String) value.get("_id"))
                            .index((String) value.get("_index"));
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

    private Map<?,?> parseline(String line) throws JsonProcessingException {
        return  OBJECT_MAPPER.readValue(line, JacksonMapper.MAP_TYPE_REFERENCE);
    }
}
