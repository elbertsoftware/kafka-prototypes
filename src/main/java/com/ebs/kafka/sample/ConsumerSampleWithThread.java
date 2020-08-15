package com.ebs.kafka.sample;

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

public class ConsumerSampleWithThread {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerSampleWithThread.class);

    private static String bootstrapServer = "localhost:9092";
    private static String groupId = "kafka_samples3";  // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group kafka_samples
    private static String topicName = "first_topic";

    private void run() {
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer runnable
        logger.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(latch, bootstrapServer, groupId, topicName);

        // start new thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {  // lambda function in Java 8
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)consumerRunnable).shutdown();

            try {
                latch.await();  // shutdown takes time
            } catch (InterruptedException e) {
                logger.error("Failed to shutdown application");
            } finally {
                logger.info("Application has exited");
            }
        }));

        // do not want the app stop, use latch to await
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static void main(String[] args) {
        new ConsumerSampleWithThread().run();
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch = null;
        KafkaConsumer<String, String> consumer = null;

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServer, String groupId, String topicName) {
            this.latch = latch;

            // create consumer configurations
            Properties configs = new Properties();
            configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer instance
            consumer = new KafkaConsumer<>(configs); // subscribe consumer to topics

            // subscribe consumer to topics
            consumer.subscribe(Collections.singleton(topicName));  // subscribe to single topic
        }

        @Override
        public void run() {
            // poll for new messages and wait for WakeUpException when consumer .wakeup() method is called
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset());
                    }
                }
            } catch(WakeupException e) {
                logger.error("Received shutdown signal");
            } finally {
                consumer.close();  // super important
                latch.countDown();  // tell the main code the consumer is done
            }
        }

        public void shutdown() {
            // the .wakeup() is a special method to interrupt consumer .poll() by throwing WakeUpException
            consumer.wakeup();
        }
    }
}
