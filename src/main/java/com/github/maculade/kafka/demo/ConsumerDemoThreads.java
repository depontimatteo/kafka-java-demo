package com.github.maculade.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args) {

        new ConsumerDemoThreads().run();

    }

    private ConsumerDemoThreads(){
        // empty constructor, just for creating object and call run() method
    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-new-group-2";
        String offsetConfig = "earliest";
        String topic = "new-topic-1";

        logger.info("Creating consumer thread!");

        // add latch for dealing with concurrency within threads
        CountDownLatch threadLatch = new CountDownLatch(1);

        // creating runnable useful for creating thread
        Runnable consumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                offsetConfig,
                topic,
                threadLatch);

        // creating and starting thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // add a shutdown hook to create a second thread for notify
        // that application has cleanly exited
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                threadLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }));

        // thread keep listening, if we raise a shutdown (so, a wakeup on the consumer)
        // the await raise an InterruptedException caused of a WakeUpException launching the latch countdown
        // and the thread safely exits and die
        try {
            threadLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        // constructor
        public ConsumerRunnable(String bootstrapServer,
                                       String groupId,
                                       String offsetConfig,
                                       String topic,
                                       CountDownLatch latch){
            this.latch = latch;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try{
                while(true){

                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String,String> record : records){
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch(WakeupException e){
                logger.info("Received shutdown signal!");
                consumer.close();
                latch.countDown();
            } finally {
                logger.info("");
            }

        }

        public void shutdown(){
            // wakeup() method is a special method useful to stop consumer poll() execution
            // throws a WakeUpException and we must surround the body of run() methon in a try/catch
            consumer.wakeup();
        }
    }
}
