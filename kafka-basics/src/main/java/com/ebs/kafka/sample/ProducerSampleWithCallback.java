package com.ebs.kafka.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerSampleWithCallback {
    private static Logger logger = LoggerFactory.getLogger(ProducerSampleWithCallback.class);

    private static String bootstrapServer = "localhost:9092";
    private static String topicName = "first_topic";

    public static void main(String[] args) {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer object
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // send data
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, String.format("Hello World %d", i));
            producer.send(record, new Callback() {
                // executed when a record is successfully sent or an exception is thrown
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Error while producing", e);
                    } else {
                        logger.info("Received new metadata:\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                }
            });
        }

        // flush data and close producer
        producer.close();
    }
}
