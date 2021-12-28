package io.kestra.plugin.elasticsearch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.*;
import org.opensearch.search.SearchModule;

import java.io.IOException;
import java.util.Collections;
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

    public static XContentParser toXContentParser(RunContext runContext, Object value, XContentType contentType) throws IllegalVariableEvaluationException, IOException {
        String json = toBody(runContext, value);

        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());

        return XContentFactory.xContent(contentType).createParser(
            new NamedXContentRegistry(searchModule.getNamedXContents()),
            LoggingDeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json
        );
    }
}
