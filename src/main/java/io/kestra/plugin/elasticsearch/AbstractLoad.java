package io.kestra.plugin.elasticsearch;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.elasticsearch.shared.BulkService;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Source file",
        description = "Kestra internal storage URI containing bulk payload; supports dynamic rendering."
    )
    @NotNull
    @PluginProperty(dynamic = true, internalStorageURI = true, group = "main")
    private String from;

    @Schema(
        title = "Bulk chunk size",
        description = "Number of operations per bulk request; default 1000."
    )
    @Default
    @PluginProperty(group = "execution")
    private Property<Integer> chunk = Property.ofValue(1000);

    abstract protected Flux<BulkOperation> source(RunContext runContext, InputStream inputStream) throws IllegalVariableEvaluationException, IOException;

    @Override
    public AbstractLoad.Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from));

        try (
            ElasticsearchClient client = this.connection.highLevelClient(runContext);
            InputStream inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)
        ) {
            Integer bufferSize = runContext.render(this.chunk).as(Integer.class).orElseThrow();
            Flux<BulkOperation> operationFlux = this.source(runContext, inputStream);

            AtomicLong count = BulkService.executeBulk(runContext, client, operationFlux, bufferSize);

            return Output.builder()
                .size(count.get())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Indexed document count",
            description = "Total operations sent across all bulk requests."
        )
        private Long size;
    }
}
