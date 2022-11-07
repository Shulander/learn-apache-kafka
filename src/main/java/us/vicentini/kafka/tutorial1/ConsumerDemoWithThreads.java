package us.vicentini.kafka.tutorial1;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThreads {
    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "first_topic";
    public static final String CONSUMER_GROUP_MY_JAVA_APPLICATION = "my-java-threads-application";


    @SneakyThrows
    public static void main(String[] args) {
        log.info("hello world");
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_MY_JAVA_APPLICATION);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumer1 = new ConsumerThread(properties, latch);
        Thread consumerT1 = new Thread(consumer1);
        consumerT1.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                log.info("Caught shutdown hook");
                consumer1.shutdown();
                latch.wait();
            }
        }));

        latch.wait();
    }

    @Slf4j
    public static class ConsumerThread implements Runnable{

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;


        public ConsumerThread(Properties properties, CountDownLatch latch) {
            this.latch = latch;
            this.consumer = new KafkaConsumer<>(properties);
        }


        @Override
        public void run() {
            try  {
                consumer.subscribe(List.of(TOPIC_NAME_FIRST_TOPIC));
                while (true) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> kafkaRecord : records) {
                        log.info("Record: key={}, value={}, partition={}, offset={}, timestamp={}", kafkaRecord.key(),
                                 kafkaRecord.value(), kafkaRecord.partition(), kafkaRecord.offset(), kafkaRecord.timestamp());
                    }
                }
            } catch (WakeupException e) {
                log.warn("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
