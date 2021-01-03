package kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticsearchConsumer {

    private static String host = "twitter-kafka-demo-5180125493.eu-central-1.bonsaisearch.net";
    private static int port = 443;
    private static String scheme = "https";
    private static String username = "veeb7pnucb";
    private static String password = "a8x36lx36k";

    public static RestHighLevelClient createClient(){
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(host, port, scheme));
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(
                    HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        String jsonString = " { \"test\": \"figo\" }";
        IndexRequest request = new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String id = response.getId();

        logger.info("Id: " + id);

        client.close();

    }
}
