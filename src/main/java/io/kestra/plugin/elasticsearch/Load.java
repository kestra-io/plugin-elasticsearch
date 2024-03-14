package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;

import java.io.BufferedReader;
import java.util.Map;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "from: \"{{ inputs.file }}\"",
                "index: \"my_index\"",
            }
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
    @PluginProperty(dynamic = false)
    private DocWriteRequest.OpType opType;

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
    protected Flux<DocWriteRequest<?>> source(RunContext runContext, BufferedReader inputStream) throws IllegalVariableEvaluationException {
        return Flux
            .create(FileSerde.reader(inputStream), FluxSink.OverflowStrategy.BUFFER)
            .map(throwFunction(o -> {
                Map<String, ?> values = (Map<String, ?>) o;

                IndexRequest indexRequest = new IndexRequest();
                if (this.index != null) {
                    indexRequest.index(runContext.render(this.getIndex()));
                }

                if (this.opType != null) {
                    indexRequest.opType(this.opType);
                }

                if (this.idKey != null) {
                    String idKey = runContext.render(this.idKey);

                    indexRequest.id(values.get(idKey).toString());

                    if (this.removeIdKey) {
                        values.remove(idKey);
                    }
                }

                indexRequest.source(values);

                return indexRequest;
            }));
    }
}
