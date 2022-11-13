package us.vicentini.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {
    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "first_topic";
    public static final String CONSUMER_GROUP_MY_JAVA_APPLICATION = "my-java-application";


    public static void main(String[] args) {
        log.info("hello world");
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_MY_JAVA_APPLICATION);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(List.of(TOPIC_NAME_FIRST_TOPIC));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> kafkaRecord : records) {
                    log.info("Record: key={}, value={}, partition={}, offset={}, timestamp={}", kafkaRecord.key(),
                             kafkaRecord.value(), kafkaRecord.partition(), kafkaRecord.offset(), kafkaRecord.timestamp());
                }
            }
        } finally {
            log.info("End execution");
        }
    }
}
