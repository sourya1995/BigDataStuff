package Kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaCricketTweetProducer {
    private static final String topic = "cricket-tweets";

    public static void main(String[] args) {
        String consumerKey = "CONSUMER-KEY";
        String consumerSecret = "CONSUMER-SECRET";
        String token = "TOKEN";
        String secret = "SECRET";

        run(consumerKey, consumerSecret, token, secret);
    }

    public static void run(String consumerKey, String consumerSecret, String token, String secret){
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("#cricket"));
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();
        try(Producer<Long, String> producer = getProducer()){
            while(true){
                ProducerRecord<Long, String> message = new ProducerRecord<>(topic, queue.take());
                producer.send(message);
                System.out.println(message.value());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.stop();
        }
    }
    private static Producer<Long, String> getProducer(){
        Properties properties = new Properties();
        properties.put("key.serializer", LongSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("bootstrap.servers", "localhost:9092");
        return new KafkaProducer<>(properties);
    }
}
