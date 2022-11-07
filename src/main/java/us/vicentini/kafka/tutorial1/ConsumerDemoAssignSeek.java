package us.vicentini.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerDemoAssignSeek {
    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "first_topic";


    public static void main(String[] args) {
        log.info("hello world");
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            //assign and seek are mostly used to replay data or fetch a specific message

            //assign
            TopicPartition partitionReadFrom = new TopicPartition(TOPIC_NAME_FIRST_TOPIC, 0);
            long offsetToReadFrom = 15L;
            int numberOPfMessageToRead=5;
            consumer.assign(List.of(partitionReadFrom));

            //seek
            consumer.seek(partitionReadFrom, offsetToReadFrom);
            while (numberOPfMessageToRead>0) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> kafkaRecord : records) {
                    log.info("Record: key={}, value={}, partition={}, offset={}, timestamp={}", kafkaRecord.key(),
                             kafkaRecord.value(), kafkaRecord.partition(), kafkaRecord.offset(), kafkaRecord.timestamp());
                    if(--numberOPfMessageToRead <= 0) {
                        break;
                    }
                }
            }
        } finally {
            log.info("End execution");
        }
    }
}
