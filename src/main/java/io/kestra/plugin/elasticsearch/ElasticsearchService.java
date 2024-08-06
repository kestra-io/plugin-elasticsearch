package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.util.Map;

public abstract class ElasticsearchService {
    @SuppressWarnings("unchecked")
    public static String toBody(RunContext runContext, Object value) throws IllegalVariableEvaluationException, IOException {
        if (value instanceof String) {
            return runContext.render((String) value);
        } else if (value instanceof Map) {
            return JacksonMapper.ofJson().writeValueAsString(runContext.render((Map<String, Object>) value));
        } else {
            throw new IllegalVariableEvaluationException("Invalid value type '" + value.getClass() + "'");
        }
    }
}
