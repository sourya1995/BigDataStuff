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
        env.setParallelism(1);
        DataStream<String> input = env.fromElements(
                "Fred", "Ginger", "Bob", "Fred", "Ginger", "Ginger", "Fred", "Carly", "Ginger"
        );
        Pattern<StockRecord, ?> pattern = Pattern.<StockRecord>begin("first").where(new SimpleCondition<StockRecord>() {
            @Override
            public boolean filter(StockRecord value) throws Exception {
                return value.getTags().contains("something");
            }
        });
        PatternStream<StockRecord> patternStream = CEP.pattern(input, pattern);
        DataStream<StockRecord> matches = patternStream.select(new PatternProcessFunction<StockRecord, String>() {

            @Override
            public void processMatch(Map<String, List<StockRecord>> map, Context context, Collector<String> collector) throws Exception {
                collector.collect(map.get("first").toString());
            }
        });

        matches.print();
        env.execute("Single Pattern Match");
    }
}
