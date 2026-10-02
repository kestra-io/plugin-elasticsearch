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
            case UPDATE, DELETE -> throw new IllegalArgumentException(
                "opType " + this + " is not supported for a single-document request, only INDEX and CREATE are; use Load or Bulk for UPDATE and DELETE"
            );
        };
    }
}
