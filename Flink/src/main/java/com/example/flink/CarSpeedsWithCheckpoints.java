package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeedsWithCheckpoints {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new FsStateBackend("file://"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000));
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if(dataStream == null){
            System.exit(1);
            return;
        }
        DataStream<String> averageViewStream = dataStream
                .map(new Speed())
                .keyBy(0)
                .flatMap(new AverageSpeed());

        averageViewStream.print();
        env.execute("Car Speeds");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {

        @Override
        public Tuple2<Integer, Double> map(String s) throws Exception {
            try {
                return Tuple2.of(1, Double.parseDouble(s));

            }catch (Exception ex) {
                System.out.println(ex);
            }
            return null;
        }
    }

    public static class AverageSpeed extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Tuple2<Integer, Double>> countSum;

        @Override
        public void flatMap(Tuple2<Integer, Double> integerDoubleTuple2, Collector<String> collector) throws Exception {
            Tuple2<Integer, Double> currentCountSum = countSum.value();
            if(integerDoubleTuple2.f1 >= 65){
                collector.collect(String.format("EXCEEDED",
                        currentCountSum.f0,
                        currentCountSum.f1 / currentCountSum.f0,
                        integerDoubleTuple2.f1));
                countSum.clear();
                currentCountSum = countSum.value();
            } else {
                collector.collect("IN LIMIT");
            }
            currentCountSum.f0 += 1;
            currentCountSum.f1 += integerDoubleTuple2.f1;
        }
    }
}
