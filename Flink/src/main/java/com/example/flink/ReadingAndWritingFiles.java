package com.example.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.concurrent.TimeUnit;

public class ReadingAndWritingFiles {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        String path = "file/path/file1.txt";
        String outputPath = "file2/path/file22.txt";
        TextInputFormat format = new TextInputFormat(new Path(path));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        DataStream<String> inputStream = env.readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
        DataStream<String> headerStream = inputStream.filter((FilterFunction<String>) input -> input.startsWith("Rio"));
        DataStream<String> filterData = inputStream.filter((FilterFunction<String>) input -> input.contains("Gold:0"));
        DataStream<String> finalStream = headerStream.union(filterData);
        final StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)
                )
                .build())
                        .build();

        finalStream.addSink(sink);
        finalStream.print();
        env.execute("Processing File Continuously");
    }
}
