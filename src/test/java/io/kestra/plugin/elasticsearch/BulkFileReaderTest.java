package io.kestra.plugin.elasticsearch;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;

import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationBase;
import co.elastic.clients.json.JsonData;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class BulkFileReaderTest {
    @Test
    void preservesConcurrencyMetadataForEveryOperation() throws Exception {
        for (var type : List.of("index", "create", "update", "delete")) {
            var operation = read(
                "{\"" + type + "\":{\"_index\":\"people\",\"_id\":\"1\",\"routing\":\"tenant\",\"if_seq_no\":0,\"if_primary_term\":4294967296}}\n" +
                    (type.equals("delete") ? "" : "{\"status\":\"new\"}\n")
            );
            var metadata = (BulkOperationBase) operation._get();
            assertThat(metadata.index(), is("people"));
            assertThat(metadata.id(), is("1"));
            assertThat(metadata.routing(), is("tenant"));
            assertThat(metadata.ifSeqNo(), is(0L));
            assertThat(metadata.ifPrimaryTerm(), is(4294967296L));
        }
    }

    @Test
    void preservesVersionMetadataForEveryOperation() throws Exception {
        for (var type : List.of("index", "create", "update", "delete")) {
            var operation = read(
                "{\"" + type + "\":{\"version\":4294967296,\"version_type\":\"external_gte\"}}\n" +
                    (type.equals("delete") ? "" : "{\"status\":\"new\"}\n")
            );
            var metadata = (BulkOperationBase) operation._get();
            assertThat(metadata.version(), is(4294967296L));
            assertThat(metadata.versionType(), is(VersionType.ExternalGte));
        }
    }

    @Test
    void preservesWriteMetadata() throws Exception {
        for (var type : List.of("index", "create")) {
            var operation = read("{\"" + type + "\":{\"pipeline\":\"normalize\",\"require_alias\":true,\"dynamic_templates\":{\"name\":\"keywords\"}}}\n{\"status\":\"new\"}\n");
            var write = type.equals("index") ? operation.index() : operation.create();
            assertThat(write.pipeline(), is("normalize"));
            assertThat(write.requireAlias(), is(true));
            assertThat(write.dynamicTemplates(), is(Map.of("name", "keywords")));
        }
    }

    @Test
    void preservesUpdateMetadata() throws Exception {
        var operation = read("{\"update\":{\"retry_on_conflict\":3,\"require_alias\":true}}\n{\"doc\":{\"status\":\"new\"}}\n").update();
        assertThat(operation.retryOnConflict(), is(3));
        assertThat(operation.requireAlias(), is(true));
    }

    @Test
    void readsNativeDocWithoutForcingUpsert() throws Exception {
        var action = read("{\"update\":{}}\n{\"doc\":{\"status\":\"new\"}}\n").update().action();
        assertThat(document(action.doc()), is(Map.of("status", "new")));
        assertThat(action.docAsUpsert(), is(nullValue()));
    }

    @Test
    void preservesNativeUpdateOptions() throws Exception {
        var action = read("""
            {"update":{}}
            {"doc":{"status":"new"},"doc_as_upsert":false,"detect_noop":false,"upsert":{"status":"first"},"_source":["status"]}
            """).update().action();
        assertThat(action.docAsUpsert(), is(false));
        assertThat(action.detectNoop(), is(false));
        assertThat(action.source().filter().includes(), is(List.of("status")));
        assertThat(document(action.upsert()), is(Map.of("status", "first")));
    }

    @Test
    void readsScriptedUpsert() throws Exception {
        var action = read("""
            {"update":{}}
            {"script":{"source":"ctx._source.count += params.n","params":{"n":2}},"scripted_upsert":true,"upsert":{"count":0}}
            """).update().action();
        assertThat(action.doc(), is(nullValue()));
        assertThat(action.docAsUpsert(), is(nullValue()));
        assertThat(action.scriptedUpsert(), is(true));
        assertThat(action.script().source().scriptString(), is("ctx._source.count += params.n"));
        assertThat(action.script().params().get("n").to(Integer.class), is(2));
    }

    @Test
    void preservesLegacyPlainDocumentUpsert() throws Exception {
        var action = read("{\"update\":{}}\n{\"status\":\"new\"}\n").update().action();
        assertThat(action.doc(), is(Map.of("status", "new")));
        assertThat(action.docAsUpsert(), is(true));
    }

    @Test
    void readsIonMetadataAndNativeUpdate() throws Exception {
        var action = read("{update:{_index:\"people\",_id:\"1\",if_seq_no:0,if_primary_term:1,routing:\"tenant\"}}\n{doc:{status:\"new\"},doc_as_upsert:true}\n").update();
        assertThat(action.ifSeqNo(), is(0L));
        assertThat(action.ifPrimaryTerm(), is(1L));
        assertThat(action.routing(), is("tenant"));
        assertThat(action.action().docAsUpsert(), is(true));
        assertThat(document(action.action().doc()), is(Map.of("status", "new")));
    }

    @Test
    void deleteDoesNotConsumeNextAction() throws Exception {
        var operations = readAll("{\"delete\":{\"_id\":\"1\"}}\n{\"index\":{\"_id\":\"2\"}}\n{\"status\":\"new\"}\n");
        assertThat(operations.size(), is(2));
        assertThat(operations.get(0).delete().id(), is("1"));
        assertThat(operations.get(1).index().document(), is(Map.of("status", "new")));
        assertThat(operations.get(1).index().ifSeqNo(), is(nullValue()));
    }

    private Object document(Object value) throws Exception {
        return value instanceof JsonData data ? JacksonMapper.toMap(data.toJson().toString()) : value;
    }

    private BulkOperation read(String content) throws Exception {
        var operations = readAll(content);
        assertThat(operations.size(), is(1));
        return operations.getFirst();
    }

    private List<BulkOperation> readAll(String content) throws Exception {
        return Flux.create(Bulk.builder().build().fileReader(new BufferedReader(new StringReader(content))))
            .collectList().block();
    }
}
