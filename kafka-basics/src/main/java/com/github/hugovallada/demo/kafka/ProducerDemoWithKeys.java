package com.github.hugovallada.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Creating a Kafka Producer.");
        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Sticky PArtitioner (Performance Improvement)
        // Se mandar várias mensagens em um curto período, o producer consegue mandar elas em batch, mandando todas pra mesma partição
        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String value = "hello_world " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Executa qnd a mensagem é enviada para o broker ou uma exception acontece.
                    if (exception == null) {
                        log.info("Received new metadata. Topic {}. Key {}. Partition {}. Offset {}. Timestamp {}.",
                                metadata.topic(), producerRecord.key(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        log.error("Error while producing: {}", exception.getMessage());
                    }
                }
            });
        }

        // Flush(synchronous) and close the producer
        //The flush will make the kafka wait until all data has been send , before close
        producer.flush();
        producer.close(); // Calling close, already flushs
    }

}
