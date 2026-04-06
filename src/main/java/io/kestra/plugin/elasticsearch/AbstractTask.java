package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @Schema(
        title = "Elasticsearch connection",
        description = "Connection settings shared by tasks; hosts are required."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected ElasticsearchConnection connection;

    @Schema(
        title = "Custom shard routing",
        description = "Optional routing key hashed to pick the shard instead of using the document id."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> routing;
}
