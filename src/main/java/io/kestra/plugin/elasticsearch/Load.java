package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.elasticsearch.model.OpType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import jakarta.validation.constraints.NotNull;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load documents in ElasticSearch using Kestra Internal Storage file."
)
@Plugin(
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
        title = "The elasticsearch index."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String index;

    @Schema(
        title = "Sets the type of operation to perform."
    )
    @PluginProperty
    private OpType opType;

    @Schema(
        title = "Use this key as id."
    )
    @PluginProperty(dynamic = true)
    private String idKey;

    @Schema(
        title = "Remove idKey from the final document."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Boolean removeIdKey = true;

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<BulkOperation> source(RunContext runContext, BufferedReader inputStream) throws IllegalVariableEvaluationException, IOException {
        return FileSerde.readAll(inputStream)
            .map(throwFunction(o -> {
                Map<String, ?> values = (Map<String, ?>) o;

                var indexRequest = new IndexOperation.Builder<Map<String, ?>>();
                if (this.index != null) {
                    indexRequest.index(runContext.render(this.getIndex()));
                }

                //FIXME
//                if (this.opType != null) {
//                    indexRequest.opType(this.opType.to());
//                }

                if (this.idKey != null) {
                    String idKey = runContext.render(this.idKey);

                    indexRequest.id(values.get(idKey).toString());

                    if (this.removeIdKey) {
                        values.remove(idKey);
                    }
                }

                indexRequest.document(values);

                var bulkOperation = new BulkOperation.Builder();
                bulkOperation.index(indexRequest.build());
                return bulkOperation.build();
            }));
    }
}
