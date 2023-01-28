package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountCountWindow {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> dataStream = StreamUtil.getDataStream(env,params);
        if(dataStream == null){
            System.exit(1);
            return;
        }

        DataStream<WordCount> wordCountStream = dataStream.flatMap(new WordCountSplitter())
                .keyBy("word")
                .countWindow(2)
                .sum("count");

        wordCountStream.print();
        env.execute("Count Window");
    }

    public static class WordCountSplitter implements FlatMapFunction<String, WordCount> {

        @Override
        public void flatMap(String sentence, Collector<WordCount> collector) throws Exception {
            for(String word: sentence.split(" ")) {
                collector.collect(new WordCount(word, 1));
            }
        }
    }

    public static class WordCount {
        public String word;
        public Integer count;

        public WordCount(){

        }

        public WordCount(String word, Integer count){
            this.word = word;
            this.count = count;
        }

        public String toString(){
            return word + ":" + count;
        }
    }
}
