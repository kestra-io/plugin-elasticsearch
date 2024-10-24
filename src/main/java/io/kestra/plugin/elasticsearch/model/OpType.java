package io.kestra.plugin.elasticsearch.model;

public enum OpType {
    INDEX,
    CREATE,
    UPDATE,
    DELETE;

    public co.elastic.clients.elasticsearch._types.OpType to() {
        return switch (this) {
            case INDEX -> co.elastic.clients.elasticsearch._types.OpType.Index;
            case CREATE -> co.elastic.clients.elasticsearch._types.OpType.Create;
            case UPDATE, DELETE -> throw new IllegalArgumentException("Only INDEX and CREATE are supported");
        };
    }
}
