package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoSpecificMessage {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoSpecificMessage.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-new-group";
        String offsetConfig = "earliest";
        String topic = "new-topic-1";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // to process a specific message we can use assign and seek
        // assign and seek are useful to replay data or fetch a specific message

        // assign
        TopicPartition partition = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L; // "L" because is a long, we are trying to process offset 15 of partition 0 for our topic
        //consumer.assign(Arrays.asList(partition));
        consumer.assign(Collections.singleton(partition));

        // seek
        consumer.seek(partition, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while(keepOnReading){

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());

                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; // exit the while loop invalidating the condition
                    break; // exit immediately from the for loop
                }
            }
        }

        logger.info("Exiting the application");
    }
}
