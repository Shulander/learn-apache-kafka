package us.vicentini.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemo {


    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "first_topic";


    public static void main(String[] args) {
        log.info("Hello World!");
        // create producer properties
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        try (var producer = new KafkaProducer<String, String>(properties)) {
            // send data
            for (int i = 0; i < 10; i++) {
                var kafkaRecord = new ProducerRecord<String, String>(TOPIC_NAME_FIRST_TOPIC, "hello world #"+i);
                producer.send(kafkaRecord);
            }
            producer.flush();
        } finally {
            log.info("End execution");
        }
    }
}
