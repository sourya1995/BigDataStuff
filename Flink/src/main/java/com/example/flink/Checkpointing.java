package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Checkpointing {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.setStateBackend(new FsStateBackend("file/path/to/file.txt"));
        DataStream<String> dataStream = env.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, Integer>> gameScores = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.split(",");
                collector.collect(Tuple2.of(tokens[0].trim(), Integer.parseInt(tokens[1].trim())));
            }
        });

        DataStream<Tuple2<String, Integer>> totalScores = gameScores.keyBy(value -> value.f0).sum(1);
        totalScores.print();
        env.execute("Game Score Computation");
    }
}
