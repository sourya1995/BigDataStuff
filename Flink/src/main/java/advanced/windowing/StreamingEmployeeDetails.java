package advanced.windowing;

import com.example.flink.JoinStreams;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingEmployeeDetails {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple2<Integer, String >> personalDetailsStream = env.socketTextStream("localhost", 8000)
                .map(new PersonalDetailsSplitter());

        DataStream<Tuple3<Integer, String, Integer>> professionalDetailsStream = env.socketTextStream("localhost", 9000)
                .map(new ProfessionalDetailsSplitter());

        DataStream<Tuple4<Integer, String, String, Integer>> joinedStream =
                personalDetailsStream.join(professionalDetailsStream)
                        .where(new PersonalKeySelector()).equalTo(new ProfessionalKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                        .apply(new EmployeeDetailJoinFunction());

        joinedStream.print();
        env.execute("Window Join Example");
    }

    private static class PersonalDetailsSplitter implements MapFunction<String, Tuple2<Integer, String>> {

        @Override
        public Tuple2<Integer, String> map(String row) throws Exception {
            String[] fields = row.split(",");
            return Tuple2.of(Integer.parseInt(fields[0].trim()), fields[1].trim());
        }


    }

    private static class ProfessionalDetailsSplitter implements MapFunction<String, Tuple3<Integer, String, Integer>> {

        @Override
        public Tuple3<Integer, String, Integer> map(String row) throws Exception {
            String[] fields = row.split(",");
            return Tuple3.of(Integer.parseInt(fields[0].trim()), fields[1].trim(), Integer.parseInt(fields[2].trim()));
        }
    }

    private static class PersonalKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {

        @Override
        public Integer getKey(Tuple2<Integer, String> value) throws Exception {
            return value.f0;
        }
    }

    private static class ProfessionalKeySelector implements KeySelector<Tuple3<Integer, String, Integer>, Integer>{

        @Override
        public Integer getKey(Tuple3<Integer, String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class EmployeeDetailJoinFunction implements JoinFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Tuple4<Integer, String, String, Integer>> {

        @Override
        public Tuple4<Integer, String, String, Integer> join(Tuple2<Integer, String> personalDetails, Tuple3<Integer, String, Integer> professionalDetails) throws Exception {
            return Tuple4.of(personalDetails.f0, personalDetails.f1, professionalDetails.f1, professionalDetails.f2);
        }
    }
}
