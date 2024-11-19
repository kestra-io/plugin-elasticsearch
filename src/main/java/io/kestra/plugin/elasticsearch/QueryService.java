package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

public abstract class QueryService {
    private static ObjectMapper MAPPER = JacksonMapper.ofJson();

    @SuppressWarnings("rawtypes")
    public static SearchRequest.Builder request(RunContext runContext, Object request) throws IllegalVariableEvaluationException, IOException {
        if (request instanceof String requestStr) {
            return parseQuery(runContext.render(requestStr));
        } else if (request instanceof Map requestMap) {
            String requestStr = runContext.render(MAPPER.writeValueAsString(requestMap));
            return parseQuery(requestStr);
        } else {
            throw new IllegalArgumentException("The `request` property must be a String or an Object");
        }
    }

    private static  SearchRequest.Builder parseQuery(String query) throws IOException {
        try (Reader reader = new StringReader(query)) {
            return new SearchRequest.Builder().withJson(reader);
        }
    }
}

