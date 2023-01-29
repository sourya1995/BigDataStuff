package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CourseScoreExtractor {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream;
        if (params.has("input")) {
            System.out.println("Executing Words example with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing Words example with socket stream");
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("Use --host and --port to specify socket");
            System.out.println("Use --input to specify file input");
            System.exit(1);
            return;
        }
        System.out.println("source initialized");
        DataStream<Tuple3<String, String, Integer>> courseScoreStream = dataStream.flatMap(new CourseScoreExtractorFn());
        courseScoreStream.print();
        env.execute("Executing scores");
    }

    public static class CourseScoreExtractorFn implements FlatMapFunction<String, Tuple3<String, String, Integer>> {

        private static final Map<Integer, String> courseLookup = new HashMap<>();

        static {
            courseLookup.put(1, "Math");
            courseLookup.put(2, "Physics");
            courseLookup.put(3, "Chemistry");
            courseLookup.put(4, "English");
        }

        @Override
        public void flatMap(String s, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
            String[] tokens = s.split(" ");
            if(tokens.length < 2){
                return;
            }

            for(Integer indexKey: courseLookup.keySet()){
                if (indexKey < tokens.length){
                    collector.collect(new Tuple3<String, String, Integer>(
                            tokens[0].trim(), courseLookup.get(indexKey),
                            Integer.parseInt(tokens[indexKey].trim())
                    ));
                }
            }
        }
    }
}
