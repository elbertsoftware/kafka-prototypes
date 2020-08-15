package com.ebs.kafka.sample;

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

public class ConsumerSampleAssignAndSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerSampleAssignAndSeek.class);

    private static String bootstrapServer = "localhost:9092";
    private static String topicName = "first_topic";

    public static void main(String[] args) {
        // create consumer configurations
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // DO NOT use groupId
        // configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // similar to --from-beginning option on CLI

        // create consumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // DO NOT subscribe consumer to any topics
        // consumer.subscribe(Collections.singleton(topicName));  // subscribe to single topic
        // consumer.subscribe(Arrays.asList(topicName));  // subscribe to multiple topics

        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topicName, 0);
        consumer.assign(Collections.singleton(partitionToReadFrom));

        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        // poll for new messages
        int numberOfMessagesToRead = 5;
        int numberOfMessagesRead = 0;
        boolean keepOnReading = true;
        while(keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + ", value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset());

                numberOfMessagesRead++;
                if(numberOfMessagesRead >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");
    }
}
