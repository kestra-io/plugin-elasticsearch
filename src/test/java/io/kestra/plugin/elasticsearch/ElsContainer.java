package io.kestra.plugin.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.kestra.core.junit.annotations.KestraTest;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

@KestraTest
public class ElsContainer {

    private static ElasticsearchContainer elasticsearchContainer;
    private static ElasticsearchClient elasticsearchClient;

//    @BeforeAll
    public static void startContainer() {
        elasticsearchContainer = new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.10.2"))
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false")
            .withExposedPorts(9200)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                cmd.getHostConfig().withPortBindings(
                    List.of(new PortBinding(
                        Ports.Binding.bindPort(9200), // Host port
                        new ExposedPort(9200)        // Container port
                    ))
                )
            ));


//        elasticsearchContainer = new FixedHostPortGenericContainer<>("docker.elastic.co/elasticsearch/elasticsearch:8.16.0")
//            .withEnv("discovery.type", "single-node")
//            .withEnv("xpack.security.enabled", "false")
//            .withExposedPorts(9200)
//            .withFixedExposedPort(9200, 9200);
        elasticsearchContainer.start();

//        RestClient restClient = RestClient.builder(
//            HttpHost.create(elasticsearchContainer.getHttpHostAddress())
//        ).build();

//        elasticsearchClient = new ElasticsearchClient(
//            new RestClientTransport(restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper())
//        );
    }

    @AfterAll
    public static void stopContainer() {
        if (elasticsearchContainer != null) {
            elasticsearchContainer.stop();
        }
    }
}
