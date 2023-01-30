package Kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamingHashtagCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(TwitterSource.CONSUMER_KEY, "CONSUMER_KEY");
        properties.setProperty(TwitterSource.CONSUMER_SECRET, "CONSUMER_SECRET");
        properties.setProperty(TwitterSource.TOKEN, "TOKEN");
        properties.setProperty(TwitterSource.TOKEN_SECRET, "TOKEN_SECRET");

        DataStream<String> streamSource = env.addSource(new TwitterSource(properties));
        DataStream<String> hashtagCounts = streamSource.flatMap(new ExtractHashtags())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> hashtagCount) throws Exception {
                        return hashtagCount.f0 + "," + hashtagCount.f1;
                    }
                });

        hashtagCounts.addSink(new FlinkKafkaProducer<>("hashtags", new SimpleStringSchema(), properties));
        env.execute("Count hashtags");

    }

    public static class ExtractHashtags implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private transient ObjectMapper jsonParser;


        @Override
        public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
            if(jsonParser == null){
                jsonParser = new ObjectMapper();
            }

            Pattern p = Pattern.compile("#\\w+");
            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);
            if(jsonNode.has("user")){
                String tweetString = jsonNode.get("text").textValue();
                Matcher matcher = p.matcher(tweetString);
                while(matcher.find()){
                    String hashtag = matcher.group(0).trim();
                    collector.collect(Tuple2.of(hashtag, 1));
                }
            }
}
