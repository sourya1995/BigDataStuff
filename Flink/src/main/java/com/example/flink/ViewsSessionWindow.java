package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ViewsSessionWindow {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if(dataStream == null){
            System.exit(1);
            return;
        }
        DataStream<Tuple3<String, String, Double>> averageViewStream = dataStream.
                map(new RowSplitter())
                .keyBy(0, 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2);
        averageViewStream.print();
        env.execute("Average Views per user, per page");
    }

    public static class RowSplitter implements MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String s) throws Exception {
            String[] fields = s.split(" ");
            if(fields.length == 3){
                return new Tuple3<String, String, Double>(
                        fields[0],
                        fields[1],
                        Double.parseDouble(fields[2]));

            }

            return null;
        }
    }
}

