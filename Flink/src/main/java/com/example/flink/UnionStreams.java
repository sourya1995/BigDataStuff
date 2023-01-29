package com.example.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStreams {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> stream1 = env.socketTextStream("localhost", 8000);
        DataStream<String> stream2 = env.socketTextStream("localhost", 9000);
        if(stream1 == null || stream2 == null){
            System.exit(1);
            return;
        }
        DataStream<String> unionStream = stream1.union(stream2);
        unionStream.print();
        env.execute("Union");


    }
}
