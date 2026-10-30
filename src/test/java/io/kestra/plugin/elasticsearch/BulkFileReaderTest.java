package io.kestra.plugin.elasticsearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;

import io.kestra.core.serializers.FileSerde;

import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Reader-level tests for Bulk action metadata and update bodies: no cluster needed, fileReader only builds operations.
class BulkFileReaderTest {

    private static final List<Map<String, Object>> ION_RECORDS = List.of(
        Map.of("index", Map.of("_index", "bulk_ion", "_id", "1")),
        Map.of("name", "ion1", "count", 1),
        Map.of("update", Map.of("_index", "bulk_ion", "_id", "1")),
        Map.of("doc", Map.of("name", "ion1-updated")),
        Map.of("delete", Map.of("_index", "bulk_ion", "_id", "2"))
    );

    private List<BulkOperation> read(String ndjson) throws Exception {
        return read(ndjson.getBytes(StandardCharsets.UTF_8));
    }

    private List<BulkOperation> read(byte[] content) throws Exception {
        return new Bulk().operations(new ByteArrayInputStream(content))
            .collectList()
            .block();
    }

    // Kestra 2.x writes binary Ion, which the Kestra 1.x FileSerde used by this build cannot produce
    static byte[] binaryIon(List<Map<String, Object>> records) throws IOException {
        var output = new ByteArrayOutputStream();
        try (var writer = IonObjectMapper.builderForBinaryWriters().build().writer().writeValues(output)) {
            for (var record : records) {
                writer.write(record);
            }
        }
        return output.toByteArray();
    }

    private static byte[] textIon(List<Map<String, Object>> records) throws IOException {
        var output = new ByteArrayOutputStream();
        for (var record : records) {
            FileSerde.write(output, record);
        }
        return output.toByteArray();
    }

    private static void assertIonOperations(List<BulkOperation> operations) {
        assertThat(operations.size(), is(3));

        var index = operations.getFirst().index();
        assertThat(index.id(), is("1"));
        assertThat(index.document(), is(Map.of("name", "ion1", "count", 1)));

        var update = operations.get(1).update();
        assertThat(update.id(), is("1"));
        assertThat(((JsonData) update.action().doc()).toJson().toString(), is("{\"name\":\"ion1-updated\"}"));
        assertThat(update.action().docAsUpsert(), nullValue());

        assertThat(operations.get(2).delete().id(), is("2"));
    }

    @Test
    void binaryIonIsRead() throws Exception {
        assertIonOperations(read(binaryIon(ION_RECORDS)));
    }

    @Test
    void textIonIsRead() throws Exception {
        assertIonOperations(read(textIon(ION_RECORDS)));
    }

    @Test
    void missingBodyFailsWithGuidance() {
        var e = assertThrows(IllegalArgumentException.class, () -> read("""
            {"index":{"_index":"bulk_guard","_id":"1"}}
            """));

        assertThat(e.getMessage(), is(
            "Bulk file ends after the 'index' action for _id '1': index, create and update actions must be followed by a body line"
        ));
    }

    @Test
    void missingBodyWithoutIdNamesTheIndex() {
        var e = assertThrows(IllegalArgumentException.class, () -> read("""
            {"create":{"_index":"bulk_guard"}}
            """));

        assertThat(e.getMessage(), is(
            "Bulk file ends after the 'create' action for index 'bulk_guard': index, create and update actions must be followed by a body line"
        ));
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

        assertThat(
            e.getMessage(), is(
                "Update body for _id '7' mixes native update fields with document fields: " +
                    "wrap the document in 'doc', or use the Load task with opType UPDATE for plain records"
            )
        );
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
    void numericMetadataValuesCoerceToStrings() throws Exception {
        var operations = read("""
            {"index":{"_index":"bulk_guard","_id":7,"routing":123,"require_alias":"true"}}
            {"status":"fresh"}
            {"create":{"_index":"bulk_guard","_id":101}}
            {"status":"created"}
            {"update":{"_index":"bulk_guard","_id":102}}
            {"field":"updated"}
            {"delete":{"_index":"bulk_guard","_id":103}}
            """);

        assertThat(operations.size(), is(4));
        var index = operations.getFirst().index();
        assertThat(index.id(), is("7"));
        assertThat(index.routing(), is("123"));
        assertThat(index.requireAlias(), is(true));

        var create = operations.get(1).create();
        assertThat(create.id(), is("101"));

        var update = operations.get(2).update();
        assertThat(update.id(), is("102"));

        var delete = operations.get(3).delete();
        assertThat(delete.id(), is("103"));
    }

    @Test
    void updateCarriesRetryOnConflict() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_guard","_id":"1","retry_on_conflict":3}}
            {"field":"value"}
            """);

        var update = operations.getFirst().update();
        assertThat(update.retryOnConflict(), is(3));
        assertThat(update.action().docAsUpsert(), is(true));
    }

    @Test
    void conditionalPlainUpdateIsNotUpserted() throws Exception {
        var operations = read("""
            {"update":{"_index":"bulk_guard","_id":"1","if_seq_no":7,"if_primary_term":2}}
            {"field":"value"}
            """);

        var update = operations.getFirst().update();
        assertThat(update.ifSeqNo(), is(7L));
        assertThat(update.ifPrimaryTerm(), is(2L));
        assertThat(update.action().doc(), is(Map.of("field", "value")));
        assertThat(update.action().docAsUpsert(), nullValue());
    }

    @Test
    void invalidMetadataNamesTheField() {
        var e = assertThrows(IllegalArgumentException.class, () -> read("""
            {"index":{"_index":"bulk_guard","_id":"1","if_seq_no":"abc"}}
            {"status":"fresh"}
            """));

        assertThat(e.getMessage(), is("Invalid 'if_seq_no' in bulk action line for _id '1': expected Long, got 'abc'"));
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

    @Test
    void unknownVersionTypeIsRejected() {
        // "force" was removed in the 9.x client; it must fail fast with a named message.
        var e = assertThrows(IllegalArgumentException.class, () -> read("""
            {"index":{"_index":"bulk_guard","_id":"1","version_type":"force"}}
            {"status":"fresh"}
            """));

        assertThat(e.getMessage(), is("Invalid version_type 'force': expected one of internal, external, external_gte"));
    }
}
