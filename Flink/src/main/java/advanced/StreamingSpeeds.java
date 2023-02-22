package advanced;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class StreamingSpeeds {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple2<String, Integer>> limits = env.addSource(new SpeedLimitSource());
        DataStream<SpeedometerReading> readings = env.addSource(new SpeedometerSource());
        SingleOutputStreamOperator<SpeedometerReading> speedReadings = readings
                .connect(limits)
                .keyBy(value -> "All cars", limit -> "All cars")
                .process(new SpeedThresholdFunction());
        speedReadings.print();
        env.execute("Speedometer readings");
    }

    public static class SpeedThresholdFunction extends KeyedCoProcessFunction<String, SpeedometerReading, Tuple2<String ,Integer>, SpeedometerReading> {
        private transient ValueState<Integer> speedLimitState;

        @Override
        public void open(Configuration parameters) throws Exception {
            speedLimitState = getRuntimeContext().getState(new ValueStateDescriptor<>("speedLimitState", Integer.class));
        }
        @Override
        public void processElement1(SpeedometerReading speedometerReading, KeyedCoProcessFunction<String, SpeedometerReading, Integer, SpeedometerReading>.Context context, Collector<SpeedometerReading> collector) throws Exception {
            Integer speedLimit = speedLimitState.value();
            if(speedLimit == null){
                speedLimit = 90;
                speedLimitState.update(speedLimit);
            }

            if(speedometerReading.speed > speedLimit) {
                System.out.println("Speed greater than the speed limit" + speedLimit + "for:" + context.getCurrentKey());
                collector.collect(speedometerReading);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Integer> speedLimitsByBrand, KeyedCoProcessFunction<String, SpeedometerReading, Integer, SpeedometerReading>.Context context, Collector<SpeedometerReading> collector) throws Exception {
            System.out.println("New speed limit for:" + speedLimitsByBrand.f0 + "is: " + speedLimitsByBrand.f1);
            speedLimitState.update(speedLimitsByBrand.f1);
        }
    }
}
