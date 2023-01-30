package Kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamingHashtags {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("tweets-with-hashtags", new SimpleStringSchema(), properties));
        DataStream<String> tweets = streamSource.flatMap(new ExtractHashtags());
        tweets.addSink(new FlinkKafkaProducer<>("hashtags", new SimpleStringSchema(), properties));
        env.execute("Streaming Hashtags");
    }

    public static class ExtractHashtags implements FlatMapFunction<String, String>{
        private transient ObjectMapper jsonParser;


        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
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
                    collector.collect(hashtag);
                }
            }
        }
    }
}
