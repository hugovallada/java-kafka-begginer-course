package com.github.hugovallada.demo.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Creating a Kafka Producer.");

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "third_application");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));



        try {
            // Subscribe consumer to our topics
            consumer.subscribe(Collections.singleton("demo_java"));

            // Pool for new Data

            while (true) {
                log.info("Pooling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (var consumerRecord : records) {
                    log.info("Key: {}, Value: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());

                }

            }
        } catch (WakeupException e) {
            log.info("Wake up exception: {}", e);
        } catch (Exception e) {
            log.error("Something happened... {}", e);
        } finally {
            log.info("Consumer is closed...");
            consumer.close();
        }


    }

}
