package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @Schema(
        title = "The connection properties."
    )
    @NotNull
    protected ElasticsearchConnection connection;

    @Schema(
        title = "Controls the shard routing of the request.",
        description = "Using this value to hash the shard and not the id."
    )
    protected Property<String> routing;
}
