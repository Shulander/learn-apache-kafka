package us.vicentini.kafka.tuturial2;


import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Tweet;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import us.vicentini.kafka.twitter.clientlib.StreamingTweetHandlerImpl;
import us.vicentini.kafka.twitter.clientlib.TweetsStreamListenersExecutor;
import us.vicentini.kafka.util.WaitKeyEvent;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


@Slf4j
public class TwitterProducer implements Runnable {


    public static final String BEARER_TOKEN = "invalid token";

    public static final String BOOTSTRAP_SERVER_LOCALHOST_9092 = "localhost:9092";
    public static final String TOPIC_NAME_FIRST_TOPIC = "twitter_tweets";


    @SneakyThrows
    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    /**
     * Set the credentials for the required APIs.
     * The Java SDK supports TwitterCredentialsOAuth2 & TwitterCredentialsBearer.
     * Check the 'security' tag of the required APIs in https://api.twitter.com/2/openapi.json in order
     * to use the right credential object.
     */
    public void run() {
        KafkaProducer<String, String> producer = createKafkaProducer();
        Consumer<Tweet> tweetConsumer =
                tweet -> {
                    var tweetRecord =
                            new ProducerRecord<>(TOPIC_NAME_FIRST_TOPIC, tweet.getAuthorId(), tweet.getText());
                    producer.send(tweetRecord);
                };
        TweetsStreamListenersExecutor tsle = createTwitterClient(tweetConsumer);

        AtomicBoolean shouldRun = new AtomicBoolean(true);
        new Thread(new WaitKeyEvent(unused -> shouldRun.set(false))).start();

        while (tsle.getError() == null && shouldRun.get()) {
            try {
                log.info("==> sleeping 5 ");
                producer.flush();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error("Interrupted error: {}", e.getMessage(), e);
            }
        }

        if (tsle.getError() != null) {
            log.error("==> Ended with error: " + tsle.getError());
        }

        log.info("Closing streaming");
        tsle.shutdown();
        producer.flush();
        producer.close();
        log.info("Done");

    }


    @NotNull
    private static KafkaProducer<String, String> createKafkaProducer() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        // create the producer
        return new KafkaProducer<>(properties);
    }


    @SneakyThrows
    @NotNull
    private TweetsStreamListenersExecutor createTwitterClient(Consumer<Tweet> consumer) {
        try {
            TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(BEARER_TOKEN));
            TweetsStreamListenersExecutor tsle = new TweetsStreamListenersExecutor();
            tsle.stream()
                    .streamingHandler(new StreamingTweetHandlerImpl(apiInstance, consumer))
                    .executeListeners();
            return tsle;
        } catch (ApiException e) {
            log.error("Status code: " + e.getCode());
            log.error("Reason: " + e.getResponseBody());
            log.error("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
            throw e;
        }
    }


}

