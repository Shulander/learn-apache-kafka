package us.vicentini.kafka.tutorial3;

import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {
    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "twitter_tweets";
    public static final String CONSUMER_GROUP_MY_JAVA_APPLICATION = "kafka-demo-elasticsearch";


    @SneakyThrows
    public static void main(String[] args) {


        try (var kafkaConsumer = createKafkaConsumer();
             var elasticSearchClient = createElasticSearchClient()) {
            while (true) {
                var records = kafkaConsumer.poll(Duration.ofMillis(5000));
                IndexRequest indexRequest = new IndexRequest("twitter");
                int recordsCount = records.count();
                log.info("Received {} records", recordsCount);

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> kafkaRecord : records) {
                    String jsonString = kafkaRecord.value().replaceAll("[\u0000-\u001f]", "");
                    String id = extractTweetId(jsonString);
                    indexRequest.source(jsonString, XContentType.JSON).id(id);
                    bulkRequest.add(indexRequest);
                }

                if (recordsCount > 0) {
                    BulkResponse response = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    if (response.hasFailures()) {
                        log.warn("Failures: {}", response.buildFailureMessage());
                    }

                    log.info("Committing offsets...");
                    kafkaConsumer.commitSync();
                    log.info("Offsets Committed!");
                }

                Thread.sleep(3000);
            }
        } finally {
            log.info("End execution");
        }

    }


    private static String extractTweetId(String jsonString) {
        return JsonParser.parseString(jsonString)
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }


    private static KafkaConsumer<String, String> createKafkaConsumer() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_MY_JAVA_APPLICATION);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(TOPIC_NAME_FIRST_TOPIC));
        return kafkaConsumer;
    }


    private static RestHighLevelClient createElasticSearchClient() {
        String hostname = "localhost";
        int port = 9200;
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port));
        return new RestHighLevelClient(builder);
    }

}
