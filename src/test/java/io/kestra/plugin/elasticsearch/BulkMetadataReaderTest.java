package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

// Reader-level tests for Bulk action metadata: no cluster needed, fileReader only builds operations.
class BulkMetadataReaderTest {

    private List<BulkOperation> read(String ndjson) throws Exception {
        var bulk = new Bulk();
        return Flux
            .create(bulk.fileReader(new BufferedReader(new StringReader(ndjson))), FluxSink.OverflowStrategy.BUFFER)
            .collectList()
            .block();
    }

    @Test
    void conditionalIndexCarriesSeqNoAndPrimaryTerm() throws Exception {
        var operations = read("""
            {"index":{"_index":"bulk_guard","_id":"1","if_seq_no":0,"if_primary_term":1}}
            {"status":"stale"}
            """);

        var index = operations.getFirst().index();
        assertThat(index.ifSeqNo(), is(0L));
        assertThat(index.ifPrimaryTerm(), is(1L));
    }

    @Test
    void indexCarriesRoutingPipelineAndVersion() throws Exception {
        var operations = read("""
            {"index":{"_index":"bulk_guard","_id":"1","routing":"shard-a","pipeline":"enrich","version":42,"version_type":"external","require_alias":true,"dynamic_templates":{"path":"full"}}}
            {"status":"fresh"}
            """);

        var index = operations.getFirst().index();
        assertThat(index.routing(), is("shard-a"));
        assertThat(index.pipeline(), is("enrich"));
        assertThat(index.version(), is(42L));
        assertThat(index.versionType(), is(VersionType.External));
        assertThat(index.requireAlias(), is(true));
        assertThat(index.dynamicTemplates(), is(Map.of("path", "full")));
    }

    @Test
    void updateCarriesRetryOnConflictAndSeqNo() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_guard","_id":"1","retry_on_conflict":3,"if_seq_no":7,"if_primary_term":2}}
            {"field":"value"}
            """);

        var update = operations.getFirst().update();
        assertThat(update.retryOnConflict(), is(3));
        assertThat(update.ifSeqNo(), is(7L));
        assertThat(update.ifPrimaryTerm(), is(2L));
    }

    @Test
    void deleteCarriesSeqNo() throws Exception {
        var operations = read("""
            {"delete":{"_index":"bulk_guard","_id":"1","if_seq_no":5,"if_primary_term":1}}
            """);

        var delete = operations.getFirst().delete();
        assertThat(delete.ifSeqNo(), is(5L));
        assertThat(delete.ifPrimaryTerm(), is(1L));
    }

    @Test
    void plainActionLineLeavesMetadataUnset() throws Exception {
        var operations = read("""
            {"index":{"_index":"bulk_guard","_id":"1"}}
            {"status":"fresh"}
            """);

        var index = operations.getFirst().index();
        assertThat(index.ifSeqNo(), nullValue());
        assertThat(index.routing(), nullValue());
        assertThat(index.versionType(), nullValue());
    }
}
