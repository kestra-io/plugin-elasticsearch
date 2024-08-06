package io.kestra.plugin.elasticsearch.model;

import org.opensearch.client.opensearch._types.Refresh;

public enum RefreshPolicy {
    IMMEDIATE,
    WAIT_UNTIL,
    NONE;

    public Refresh to() {
        return switch (this) {
            case NONE -> Refresh.False;
            case IMMEDIATE -> Refresh.True;
            case WAIT_UNTIL -> Refresh.WaitFor;
        };
    }
}
