package io.kestra.plugin.elasticsearch.model;

public enum OpType {
    INDEX,
    CREATE,
    UPDATE,
    DELETE;

    public org.opensearch.client.opensearch._types.OpType to() {
        return switch (this) {
            case INDEX -> org.opensearch.client.opensearch._types.OpType.Index;
            case CREATE -> org.opensearch.client.opensearch._types.OpType.Create;
            case UPDATE, DELETE -> throw new IllegalArgumentException("Only INDEX and CREATE are supported");
        };
    }
}
