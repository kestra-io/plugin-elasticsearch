package io.kestra.plugin.elasticsearch;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load documents in elasticsearch using [bulk files](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) elastic files"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  hosts: ",
                "   - \"http://localhost:9200\"",
                "from: \"{{ inputs.file }}\""
            }
        )
    }
)
public class Bulk extends AbstractLoad implements RunnableTask<Bulk.Output> {
    @Override
    protected Flux<DocWriteRequest<?>> source(RunContext runContext, BufferedReader inputStream) throws IOException {
        return Flux
            .create(this.esNdJSonReader(inputStream), FluxSink.OverflowStrategy.BUFFER);
    }

    @SuppressWarnings("unchecked")
    public Consumer<FluxSink<DocWriteRequest<?>>> esNdJSonReader(BufferedReader input) throws IOException {
        return throwConsumer(s -> {
            String row;

            while ((row = input.readLine()) != null) {
                Map.Entry<String, Object> operation = JacksonMapper.toMap(row).entrySet().iterator().next();
                Map<String, Object> value = (Map<String, Object>) operation.getValue();

                DocWriteRequest<?> docWriteRequest;

                switch (operation.getKey()) {
                    case "index":
                        docWriteRequest = new IndexRequest()
                            .id((String) value.get("_id"))
                            .source(input.readLine(), XContentType.JSON);
                        break;
                    case "create":
                        docWriteRequest = new IndexRequest()
                            .id((String) value.get("_id"))
                            .opType(DocWriteRequest.OpType.CREATE)
                            .source(input.readLine(), XContentType.JSON);
                        break;
                    case "update":
                        docWriteRequest = new UpdateRequest()
                            .id((String) value.get("_id"))
                            .docAsUpsert(true)
                            .doc(input.readLine(), XContentType.JSON);
                        break;
                    case "delete":
                        docWriteRequest = new DeleteRequest()
                            .id((String) value.get("_id"));
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid bulk request type on '" + row + "'");
                }


                if (value.containsKey("_index")) {
                    docWriteRequest.index((String) value.get("_index"));
                }

                s.next(docWriteRequest);
            }

            s.complete();
        });
    }
}
