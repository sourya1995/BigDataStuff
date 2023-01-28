package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.module.Configuration;

public class CarSpeeds {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if(dataStream == null){
            System.exit(1);
            return;
        }
        DataStream<String> averageViewStream = dataStream
                .map(new Speed())
                        .keyBy(0)
                                .flatMap(new AverageSpeedValueState());
        env.execute("Car Speeds");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>>{

        @Override
        public Tuple2<Integer, Double> map(String s) throws Exception {
            return Tuple2.of(1, Double.parseDouble(s));
        }
    }

    private static transient ValueState<Tuple2<Integer, Double>> countSumState;
    public static class AverageSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        @Override
        public void flatMap(Tuple2<Integer, Double> integerDoubleTuple2, Collector<String> collector) throws Exception {
            Tuple2<Integer, Double> currentCountSum = countSumState.value();
            if(integerDoubleTuple2.f1 >= 65){
                collector.collect(String.format("EXCEEDED!!",
                        currentCountSum.f0,
                        currentCountSum.f1 / currentCountSum.f0,
                        integerDoubleTuple2.f1)
                );
            } else {
                collector.collect("IN LIMIT");
            }
            currentCountSum.f0 += 1;
            currentCountSum.f1 += integerDoubleTuple2.f1;
            countSumState.update(currentCountSum);
        }

        public void open(Configuration config){
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<Tuple2<Integer, Double>>("carAverageSpeed",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
                    }), Tuple2.of(0, 0.0));
            countSumState = getRuntimeContext().getState(descriptor);
        }
    }
}
