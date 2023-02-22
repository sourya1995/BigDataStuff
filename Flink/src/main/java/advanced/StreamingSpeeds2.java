package advanced;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class StreamingSpeeds2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> broadcastLimits = env.addSource(new SpeedLimitSource());
        final MapStateDescriptor<String, Tuple2<String, Integer>> broadcastStateDescriptor = new MapStateDescriptor<>("LimitsBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {

        }));
        BroadcastStream<Tuple2<String, Integer>> limitsBroadcastStream = broadcastLimits.broadcast(broadcastStateDescriptor);
        KeyedStream<SpeedometerReading, String> readings = env.addSource(new SpeedometerSource())
                .keyBy(value -> value.car);

        SingleOutputStreamOperator<SpeedometerReading> speedReadings = readings.connect(limitsBroadcastStream)
                .process(new StreamingSpeeds.SpeedThresholdFunction(broadcastStateDescriptor));

        speedReadings.print();
        env.execute("Speedometer readings");

    }

    public static class SpeedThresholdFunction extends KeyedBroadcastProcessFunction<String, SpeedometerReading, Tuple2<String, Integer>, SpeedometerReading> {
        MapStateDescriptor<String, Tuple2<String, Integer>> broadcastStateDescriptor>;

        public SpeedThresholdFunction(
                MapStateDescriptor<String, Tuple2<String, Integer>> broadcastStateDescriptor) {
            this.broadcastStateDescriptor = broadcastStateDescriptor;
        }

        @Override
        public void processElement(SpeedometerReading speedometerReading, KeyedBroadcastProcessFunction<String, SpeedometerReading, Tuple2<String, Integer>, SpeedometerReading>.ReadOnlyContext readOnlyContext, Collector<SpeedometerReading> collector) throws Exception {
            Tuple2<String, Integer> speedLimitTuple = readOnlyContext
                    .getBroadcastState(broadcastStateDescriptor)
                    .get(readOnlyContext.getCurrentKey());

            int speedLimit = 90;

            if(speedLimitTuple != null){
                speedLimit = speedLimitTuple.f1;
            }

            if(speedometerReading.speed > speedLimit){
                System.out.println("Speed greater than the current speed limit:" + speedLimit + "for:" + readOnlyContext.getCurrentKey());
                collector.collect(speedometerReading);
            }
        }

        @Override
        public void processBroadcastElement(Tuple2<String, Integer> speedLimitsByBrand, KeyedBroadcastProcessFunction<String, SpeedometerReading, Tuple2<String, Integer>, SpeedometerReading>.Context context, Collector<SpeedometerReading> collector) throws Exception {
            System.out.println("New speed limit for:" + speedLimitsByBrand.f0 + "is: " + speedLimitsByBrand.f1);
            context.getBroadcastState(broadcastStateDescriptor)
                    .put(speedLimitsByBrand.f0, speedLimitsByBrand);
        }
    }
}
