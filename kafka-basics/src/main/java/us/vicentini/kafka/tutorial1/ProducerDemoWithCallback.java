package us.vicentini.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {


    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "first_topic";


    public static void main(String[] args) {
        log.info("Hello World!");
        // create producer properties
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        // create the producer
        try (var producer = new KafkaProducer<String, String>(properties)) {
            // send data
            for (int i = 0; i < 10; i++) {
                var kafkaRecord = new ProducerRecord<String, String>(TOPIC_NAME_FIRST_TOPIC, "hello world #"+i);
                producer.send(kafkaRecord, (RecordMetadata metadata, Exception exception) -> {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        log.info("Callback metadata: {}", toStringMetadata(metadata));
                    } else {
                        log.error("Error while producing!!! {}", exception.getMessage(), exception);
                    }
                });
            }
            producer.flush();
        } finally {
            log.info("End execution");
        }
    }


    private static String toStringMetadata(RecordMetadata metadata) {
        return String.format("RecordMetadata(topic=%s, partition=%d, offset=%d, timestamp=%d)", metadata.topic(),
                             metadata.partition(),
                             metadata.offset(),
                             metadata.timestamp());
    }
}
