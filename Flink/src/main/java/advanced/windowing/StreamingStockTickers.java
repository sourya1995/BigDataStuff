package advanced.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class StreamingStockTickers {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<StockPrices> stockStream = env
                .addSource(new StockTickerSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StockPrices>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.timestamp))
                .keyBy(value -> value.ticker)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .aggregate(new AverageAggregate());

        stockStream.print();
        env.execute("Stock prices");

    }

    private static class AverageAggregate implements AggregateFunction<StockPrices, Tuple3<String, Double, Long>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Long> createAccumulator() {
            return new Tuple3<>("placeholder", 0.0, 0L);
        }

        @Override
        public Tuple3<String, Double, Long> add(StockPrices value, Tuple3<String, Double, Long> accumulator) {
            return new Tuple3<>(value.ticker, accumulator.f1 + value.price + accumulator.f2 + 1L);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> accumulator) {
            return new Tuple2<String, Double>(accumulator.f0, (accumulator.f1 / accumulator.f2));
        }

        @Override
        public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> a, Tuple3<String, Double, Long> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
