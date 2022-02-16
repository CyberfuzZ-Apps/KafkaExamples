package ru.cyberfuzz.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Класс ProducerDemoWithCallback
 *
 * @author Evgeniy Zaytsev
 * @version 1.0
 */
public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create ProducerRecord
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first_topic",
                        "HELLO FROM INTELLIJ IDEA WITH CALLBACK!");

        // send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //execute every time a record is successfully sent or an exception is throws
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata." + System.lineSeparator()
                            + "Topic: " + recordMetadata.topic() + System.lineSeparator()
                            + "Partition: " + recordMetadata.partition() + System.lineSeparator()
                            + "Offset: " + recordMetadata.offset() + System.lineSeparator()
                            + "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing! ", e);
                }
            }
        });

        // flush data
        producer.flush();

        // flush and close Producer
        producer.close();
    }
}
