package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Reader-level tests for Bulk update bodies: no cluster needed, fileReader only builds operations.
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
        var action = operations.getFirst().update().action();
        assertThat(((JsonData) action.doc()).toJson().toString(), is("{\"status\":\"new\"}"));
        assertThat(action.docAsUpsert(), nullValue());
    }

    @Test
    void plainBodyKeepsLegacyUpsertBehavior() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"field2":"value2"}
            """);

        var action = operations.getFirst().update().action();
        assertThat(action.doc(), is(Map.of("field2", "value2")));
        assertThat(action.docAsUpsert(), is(true));
    }

    @Test
    void scriptAndUpsertOptionsAreHonored() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"script":{"source":"ctx._source.counter += params.n","params":{"n":2}},"upsert":{"counter":0},"scripted_upsert":true}
            """);

        var action = operations.getFirst().update().action();
        assertThat(action.script(), notNullValue());
        assertThat(action.script().source().scriptString(), is("ctx._source.counter += params.n"));
        assertThat(((JsonData) action.upsert()).toJson().toString(), is("{\"counter\":0}"));
        assertThat(action.scriptedUpsert(), is(true));
        assertThat(action.doc(), nullValue());
    }

    @Test
    void explicitDocAsUpsertFalseIsRespected() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_update","_id":"1"}}
            {"doc":{"status":"new"},"doc_as_upsert":false}
            """);

        var action = operations.getFirst().update().action();
        assertThat(((JsonData) action.doc()).toJson().toString(), is("{\"status\":\"new\"}"));
        assertThat(action.docAsUpsert(), is(false));
    }

    @Test
    void mixedNativeAndDocumentFieldsFailWithGuidance() {
        var e = assertThrows(IllegalArgumentException.class, () -> read("""
            {"update":{"_index":"bulk_update","_id":"7"}}
            {"doc":{"status":"new"},"unrelated_field":"x"}
            """));

        assertThat(e.getMessage(), is(
            "Update body for _id '7' mixes native update fields with document fields: " +
                "wrap the document in 'doc', or use the Load task with opType UPDATE for plain records"
        ));
    }
}
