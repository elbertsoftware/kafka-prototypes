package com.ebs.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerSample {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerSample.class);

    private static String bootstrapServer = "localhost:9092";
    private static String groupId = "kafka_samples";  // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group kafka_samples
    private static String topicName = "first_topic";

    public static void main(String[] args) {
        // create consumer configurations
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // similar to --from-beginning option on CLI

        // create consumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // subscribe consumer to topics
        // consumer.subscribe(Collections.singleton(topicName));  // subscribe to single topic
        consumer.subscribe(Arrays.asList(topicName));  // subscribe to multiple topics

        // poll for new messages
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + ", value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset());
            }
        }
    }
}
