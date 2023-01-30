package Kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class StreamingSquareRoots {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<>("numbers", new SimpleStringSchema(), properties));
        DataStream<String> result = dataStream.map(new SquareRoot());
        result.addSink(new FlinkKafkaProducer<>("square-roots", new SimpleStringSchema(), properties));
        env.execute("Streaming Square Roots");
    }

    public static class SquareRoot implements MapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            try {
                double n = Double.parseDouble(s);
                double result = Math.sqrt(n);
                return "Square root of " + s + ":" + result;
            } catch(Exception ex){
                return "Invalid entry, input was not a numeric value";
            }
        }
    }
}
