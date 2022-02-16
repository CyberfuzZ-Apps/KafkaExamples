package ru.cyberfuzz.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Класс ProducerDemoKeys
 *
 * @author Evgeniy Zaytsev
 * @version 1.0
 */
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create ProducerRecord
            String topic = "first_topic";
            String value = "Hello world " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key); // log the key

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
            })
                    .get(); //block the .send() to make it synchronous - DON'T DO THIS IN PRODUCTION!!!
        }


        // flush data
        producer.flush();

        // flush and close Producer
        producer.close();
    }
}
