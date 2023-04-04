package CEP;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromElements(
                "Fred", "Ginger", "Bob", "Fred", "Ginger", "Ginger", "Fred", "Carly", "Ginger"
        );
        Pattern<String, ?> pattern = Pattern.<String>begin("first").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.equals("Fred");
            }
        }).next("second").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.equals("Ginger");
            }
        }).next("third").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.equals("Bob");
            }
        });
        PatternStream<String> patternStream = CEP.pattern(input, pattern);
        DataStream<String> matches = patternStream.select(new PatternProcessFunction<String, String>() {

            @Override
            public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {
                collector.collect(map.get("first").toString() + " -> " + map.get("second").toString() + map.get("third").toString() );
            }
        });

        matches.print();
        env.execute("Single Pattern Match");
    }
}
