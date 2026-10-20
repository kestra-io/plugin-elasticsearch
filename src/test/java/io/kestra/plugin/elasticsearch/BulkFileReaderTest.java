package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link Bulk#fileReader}: update bodies carrying native
 * Elasticsearch update fields must be replayed with bulk API semantics,
 * while plain partial documents keep the historical upsert behavior.
 * No cluster is needed: the reader only builds {@link BulkOperation}s.
 */
class BulkFileReaderTest {

    private List<BulkOperation> read(String ndjson) throws Exception {
        var bulk = new Bulk();
        return Flux
            .create(bulk.fileReader(new BufferedReader(new StringReader(ndjson))), FluxSink.OverflowStrategy.BUFFER)
            .collectList()
            .block();
    }

    @Test
    void nativeDocBodyIsNotNestedAsSource() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"doc":{"status":"new"}}
            """);

        assertThat(operations.size(), is(1));
        var action = operations.get(0).update().action();
        // The doc must be the partial document itself, not the whole body.
        assertThat(action.doc(), is(Map.of("status", "new")));
        // No upsert was requested, so none may be forced.
        assertThat(action.docAsUpsert(), nullValue());
    }

    @Test
    void plainBodyKeepsLegacyUpsertBehavior() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"field2":"value2"}
            """);

        var action = operations.get(0).update().action();
        assertThat(action.doc(), is(Map.of("field2", "value2")));
        assertThat(action.docAsUpsert(), is(true));
    }

    @Test
    void scriptAndUpsertOptionsAreHonored() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"script":{"source":"ctx._source.counter += params.n","params":{"n":2}},"upsert":{"counter":0},"scripted_upsert":true}
            """);

        var action = operations.get(0).update().action();
        assertThat(action.script(), notNullValue());
        assertThat(action.script().source(), is("ctx._source.counter += params.n"));
        assertThat(action.upsert(), is(Map.of("counter", 0)));
        assertThat(action.scriptedUpsert(), is(true));
        assertThat(action.doc(), nullValue());
    }

    @Test
    void explicitDocAsUpsertFalseIsRespected() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"doc":{"status":"new"},"doc_as_upsert":false}
            """);

        var action = operations.get(0).update().action();
        assertThat(action.doc(), is(Map.of("status", "new")));
        assertThat(action.docAsUpsert(), is(false));
    }
}
