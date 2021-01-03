package com.github.maculade.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String twitterApiKey = "UhN0asN6RW6k9nLXgZbyb336Z";
    private String twitterApiKeySecret = "XHT88AZ8VKNuSwbGWJkeTFJHVwBV4fGCiC6m9rSgDrYcw97YhK";
    private String twitterAccessToken = "750225429936603136-n9AEVblI5W9jSMsfLV3AHeTPTxAHtbv";
    private String twitterAccessTokenSecret = "OJMvtH2g8SvT50pd9HpraGOPoWwgJSgsHI2IWs428ZzY7";

    private List<String> terms = Lists.newArrayList("usa", "covid", "vaxine");

    public TwitterProducer(){}

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run(){

        // The stream is
        // poll tweets from twitter --> send tweets to kafka with producer -->
        // --> process kafka messages with the consumer --> put messages into elasticsearch


        // create Twitter Client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        twitterClient.connect();

        // create Kafka producer
        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

        // create shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing application...");
            logger.info("Stopping Twitter Client...");
            twitterClient.stop();
            logger.info("Closing Kafka Producer");
            kafkaProducer.close();
        }));

        // loop for getting Tweets and send them to Kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.info("Exiting from Twitter client");
                twitterClient.stop();
            }

            if(msg != null){
                logger.info("Msg: " + msg);
                kafkaProducer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Exception Error", e);
                        }
                    }
                });
            }
        }

        logger.info("End of application");

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(twitterApiKey, twitterApiKeySecret, twitterAccessToken, twitterAccessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // add safety producer properties (idempotence and zero data loss due to network problems)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // these safety values are explicitly added for clearance, but are the defaults with idempotence enabled
        // I always want an ACK from the broker
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // I set the retries number to the max value possible
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // I'm using Kafka 2.0 so I can put 5 in this property, otherwise I must put 1
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //  add high throughput properties (at the expense of a little bit of latency -ms- and CPU usage for compression)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // best performance compression algorythm, made by Google
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 16 KB batch size messages


        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }
}
