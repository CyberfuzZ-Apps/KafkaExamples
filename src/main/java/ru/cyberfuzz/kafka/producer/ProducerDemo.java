package ru.cyberfuzz.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Класс ProducerDemo
 *
 * @author Evgeniy Zaytsev
 * @version 1.0
 */
public class ProducerDemo {

    public static void main(String[] args) {
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
                new ProducerRecord<>("first_topic", "HELLO FROM INTELLIJ IDEA!");

        // send data - asynchronous
        producer.send(record);

        // flush data
        producer.flush();

        // flush and close Producer
        producer.close();
    }
}
