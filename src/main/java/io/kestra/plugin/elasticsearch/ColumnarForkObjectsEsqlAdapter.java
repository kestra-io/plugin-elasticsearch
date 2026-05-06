package io.kestra.plugin.elasticsearch;

import co.elastic.clients.ApiClient;
import co.elastic.clients.elasticsearch._helpers.esql.EsqlAdapter;
import co.elastic.clients.elasticsearch._helpers.esql.objects.ObjectsEsqlAdapter;
import co.elastic.clients.elasticsearch.esql.QueryRequest;
import co.elastic.clients.json.*;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BinaryResponse;
import jakarta.annotation.Nullable;
import jakarta.json.stream.JsonParser;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Fork of {@link ObjectsEsqlAdapter} due to unreleased change
 * <p>
 * Adapter for "columnar" queries
 *
 * @see <a href="https://github.com/elastic/elasticsearch-java/blob/main/java-client/src/main/java/co/elastic/clients/elasticsearch/_helpers/esql/EsqlAdapterBase.java">Unreleased change</a>
 */
public class ColumnarForkObjectsEsqlAdapter<T> implements EsqlAdapter<Iterable<T>> {

    public static <T> ColumnarForkObjectsEsqlAdapter<T> of(Class<T> clazz) {
        return new ColumnarForkObjectsEsqlAdapter<>(clazz);
    }

    public static <T> ColumnarForkObjectsEsqlAdapter<T> of(Type type) {
        return new ColumnarForkObjectsEsqlAdapter<>(type);
    }

    private final Type type;

    public ColumnarForkObjectsEsqlAdapter(Type type) {
        this.type = type;
    }

    @Override
    public String format() {
        return "json";
    }

    @Override
    public boolean columnar() {
        return true;
    }

    @Override
    public Iterable<T> deserialize(ApiClient<ElasticsearchTransport, ?> client, QueryRequest request, BinaryResponse response)
        throws IOException {
        JsonpMapper mapper = client._jsonpMapper();

        if (!(mapper instanceof BufferingJsonpMapper)) {
            throw new IllegalArgumentException("ES|QL object mapping currently only works with JacksonJsonpMapper");
        }

        JsonParser parser = mapper.jsonProvider().createParser(response.content());
        List<ForkObjectsEsqlAdapter.EsqlMetadata.EsqlColumn> columns = readHeader(parser, mapper).columns;
        List<T> results = new ArrayList<>();

        columns.forEach(column -> results.add(parseRow(column, parser, mapper)));

        return results;
    }


    private T parseRow(ForkObjectsEsqlAdapter.EsqlMetadata.EsqlColumn column, JsonParser parser, JsonpMapper mapper) {
        // FIXME: add a second implementation not requiring a buffering parser
        BufferingJsonGenerator buffer = ((BufferingJsonpMapper) mapper).createBufferingGenerator();

        buffer.writeStartObject();

        buffer.writeKey(column.name());
        JsonpUtils.copy(parser, buffer);

        buffer.writeEnd();

        return mapper.deserialize(buffer.getParser(), type);
    }


    public static class EsqlMetadata extends co.elastic.clients.elasticsearch._helpers.esql.EsqlMetadata {
        @Nullable
        public Long took;
    }

    /**
     * Reads the header of an ES|QL response, moving the parser at the beginning of the first value row.
     * The caller can then read row arrays until finding an end array that closes the top-level array.
     */
    public static EsqlMetadata readHeader(JsonParser parser, JsonpMapper mapper) {
        EsqlMetadata result = new EsqlMetadata();

        JsonpUtils.expectNextEvent(parser, JsonParser.Event.START_OBJECT);

        parse:
        while (JsonpUtils.expectNextEvent(parser, JsonParser.Event.KEY_NAME) != null) {
            switch (parser.getString()) {
                case "values": {
                    // We're done parsing header information
                    break parse;
                }
                case "columns": {
                    result.columns = JsonpDeserializer
                        .arrayDeserializer(EsqlMetadata.EsqlColumn._DESERIALIZER)
                        .deserialize(parser, mapper);
                    break;
                }
                case "took": {
                    JsonpUtils.expectNextEvent(parser, JsonParser.Event.VALUE_NUMBER);
                    result.took = parser.getLong();
                    break;
                }
                default: {
                    // Ignore everything else
                    JsonpUtils.skipValue(parser);
                    break;
                }
            }
        }

        if (result.columns == null) {
            throw new JsonpMappingException("Expecting a 'columns' property before 'values'.", parser.getLocation());
        }

        // Beginning of the `values` property
        JsonpUtils.expectNextEvent(parser, JsonParser.Event.START_ARRAY);

        return result;
    }
}
