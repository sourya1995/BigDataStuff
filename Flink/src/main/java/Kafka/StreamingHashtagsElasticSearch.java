package Kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.Requests;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamingHashtagsElasticSearch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStream<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("tweets-with-hashtags", new SimpleStringSchema(), properties));
        streamSource
                .flatMap(new ExtractText())
                .addSink((SinkFunction<String>) getElasticsearchSink());

        streamSource
                .flatMap(new ExtractHashtags())
                .addSink(new FlinkKafkaProducer<>("hashtags", new SimpleStringSchema(), properties));

        env.execute("Streaming Hashtags");
    }

    public static class ExtractHashtags implements FlatMapFunction<String, String> {

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String tweetRecord, Collector<String> out) throws Exception {
            if(jsonParser == null){
                jsonParser = new ObjectMapper();
            }

            Pattern p = Pattern.compile("#\\w+");
            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);
            if(jsonNode.has("user")) {
                String tweetString = jsonNode.get("text").textValue();
                Matcher matcher = p.matcher(tweetString);
                while(matcher.find()) {
                    String hashtag = matcher.group(0).trim();
                    out.collect(hashtag);
                }
            }

        }
    }

    public static class ExtractText implements FlatMapFunction<String, String> {

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String tweetRecord, Collector<String> out) throws Exception {
            if(jsonParser == null){
                jsonParser = new ObjectMapper();
            }

            JsonNode jsonNode = jsonParser.readValue(tweetRecord, JsonNode.class);
            if(jsonNode.has("user")) {
                String tweetString = jsonNode.get("text").textValue();
                out.collect(tweetString);
            }
        }
    }

    private static ElasticsearchSink<String> getElasticsearchSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
            httpHosts,
            new ElasticsearchSinkFunction<String>(){


                public IndexRequest createIndexRequest(String element){
                    Map<String, String> json = new HashMap<>();
                    json.put("data", element);
                    return Requests.indexRequest()
                            .index("tweets")
                            .source(json);
                }


                @Override
                public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                    requestIndexer.add(createIndexRequest(element));

                }
            }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
        final Header[] defaultHeaders = new Header[] {
                new BasicHeader("Content-Type", "application/json")
        };

        esSinkBuilder
                .setRestClientFactory(
                        (RestClientFactory) restClientBuilder -> restClientBuilder.setDefaultHeaders(defaultHeaders)
                                .setRequestConfigCallback(
                                        (RestClientBuilder.RequestConfigCallback) builder -> builder.setSocketTimeout(10000)
                                )
                );

        return esSinkBuilder.build();

    }


}
