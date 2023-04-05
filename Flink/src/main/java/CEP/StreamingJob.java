package CEP;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockRecord> input = env.fromElements(
                new StockRecord("MSFT", new DateTime("2020-01-02T00:00", DateTimeZone.UTC).toInstant(), 157.39f, "momentum"),
                new StockRecord("MSFT", new DateTime("2020-01-03T00:00", DateTimeZone.UTC).toInstant(), 157.4f, "momentum"),
                new StockRecord("MSFT", new DateTime("2020-01-04T00:00", DateTimeZone.UTC).toInstant(), 157.52f, "flat"),
                new StockRecord("MSFT", new DateTime("2020-01-05T00:00", DateTimeZone.UTC).toInstant(), 157.39f, "flat")
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<StockRecord>forBoundedOutOfOrderness(Duration.ofDays(0))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.noSkip();
        Pattern<StockRecord, ?> pattern = Pattern.<StockRecord>begin("start")
                .where(new SimpleCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord) throws Exception {
                        return stockRecord.getTags().contains("momentum");
                    }
                }).where(new SimpleCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord) throws Exception {
                        return stockRecord.getClosingPrice() > 1450;
                    }
                }).oneOrMore().until(new SimpleCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord) throws Exception {
                        return stockRecord.getTags().contains("something more");
                    }
                }).followedBy("next").where(new SimpleCondition<StockRecord>() {
                    @Override
                    public boolean filter(StockRecord stockRecord) throws Exception {
                        return stockRecord.getTags().contains("flat");
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
                }).oneOrMore().within(Time.days(5));
        PatternStream<StockRecord> patternStream = CEP.pattern(input, pattern);
        final OutputTag<String> outputTag = new OutputTag<String>("timed-out-matches") {};
        SingleOutputStreamOperator<String> matches = patternStream.process(new StockPatternProcessFunction(outputTag));


        matches.print();
        DataStream<String> timedOutMatches = matches.getSideOutput(outputTag);
        timedOutMatches.print();
        env.execute("Stock Streaming");
    }

    private static class StockPatterbProcessFunction extends PatternProcessFunction<StockRecord, String> implements TimedOutPartialMatchHandler<StockRecord> {

        private final OutputTag<String> outputTag;

        private StockPatterbProcessFunction(OutputTag<String> outputTag) {
            this.outputTag = outputTag;
        }

        @Override
        public void processMatch(Map<String, List<StockRecord>> map, Context context, Collector<String> collector) throws Exception {
            collector.collect("***" + map.toString());
        }

        @Override
        public void processTimedOutMatch(Map<String, List<StockRecord>> map, Context context) throws Exception {
            context.output(outputTag, "---" + map.toString());
        }
    }
}
