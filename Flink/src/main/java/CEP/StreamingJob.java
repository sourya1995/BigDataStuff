package CEP;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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

        DataStream<StockRecord> input = env.readTextFile("/path/to/file")
                .map(new ConvertToStockRecordFn());
        Pattern<StockRecord, ?> pattern = Pattern.<StockRecord>begin("start")
                .where(new SimpleCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord) throws Exception {
                        return false;
                    }
                })
                .next("middle").where(new IterativeCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord, Context<StockRecord> context) throws Exception {
                        double sum = 0;
                        int count = 0;

                        for(StockRecord record: context.getEventsForPattern("start")) {
                            sum += record.getClosingPrice();
                            count++;
                        }

                        return (sum/count) > 1440;
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

    private static class ConvertToStockRecordFn implements MapFunction<String, StockRecord> {

        @Override
        public StockRecord map(String s) throws Exception {
            String[] tokens = s.split(",");
            String ticker = tokens[8].trim();

            if(ticker.equals("AMZN") || ticker.equals("MSFT") || ticker.equals("GOOG")){
                return new CloudPlatformStockRecord(ticker, tokens[0].trim(), Float.parseFloat(tokens[5].trim()), tokens[7].trim());
            }
            return new StockRecord(tokens[8].trim(), tokens[0].trim(), Float.parseFloat(tokens[5].trim()), tokens[7].trim());

        }
    }
}
