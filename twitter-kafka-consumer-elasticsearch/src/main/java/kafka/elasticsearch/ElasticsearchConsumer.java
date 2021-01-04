package kafka.elasticsearch;

import com.google.gson.Gson;
import javafx.beans.binding.ObjectExpression;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

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

    public static KafkaConsumer<String, String> createElasticsearchConsumer(){

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elaasticsearch";
        String offsetConfig = "earliest";
        String topic = "twitter-tweets";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
        // manually commit offset cause I need to save the commit of the offset after I put data into elasticsearch
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // I want to process only a bunch of 10 messages every poll made by my consumer
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    private static String getTweetId(String record) {

        Gson gson = new Gson();

        HashMap<String, Object> tweet_obj = gson.fromJson(record, HashMap.class);

        return tweet_obj.get("id_str").toString();

    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> elasticsearchConsumer = createElasticsearchConsumer();

        while(true){

            // I'm polling the topic with my consumer

            ConsumerRecords<String,String> records = elasticsearchConsumer.poll(Duration.ofMillis(100));
            Integer recordsCount = records.count();
            BulkRequest bulkRequest = new BulkRequest();

            logger.info("Received " + recordsCount.toString() + " tweets");

            for(ConsumerRecord<String,String> record : records){

                try {
                    // Due to avoid duplicates, I need to add an ID while putting data into Elasticsearch
                    // I have two strategies:
                    // - use a generic Kafka unique string Id with topic + partition + offset mixed
                    // - use the tweet id coming from data processed

                    // Generic Kafka id
                    //String dataId = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Tweet id
                    String dataId = getTweetId(record.value());

                    //logger.info("Tweet Id: " + dataId);

                    // And then for every record I put the JSON processed into an IndexRequest object ready to put data into elasticsearch
                    IndexRequest request = new IndexRequest(
                            "twitter",
                            "tweets",
                            dataId
                    ).source(record.value(), XContentType.JSON);

                    // I add my IndexRequest object with data into a BulkRequest container
                    bulkRequest.add(request);
                }
                catch(NullPointerException e){
                    logger.error("Error Exception: ", e);
                }
            }

            if(recordsCount > 0) {
                // Now I execute my BulkRequest container of IndexRequests, that submit to elasticsearch all the requests.
                // Every BulkRequest container contains MAX_POLL_RECORDS_CONFIG records
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Manual committing offsets into state Kafka table");
                elasticsearchConsumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();

    }

}
