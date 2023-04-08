package Beam;

import advanced.StreamingGameScores;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputation {
    private static final String CSV_HEADER = "id, name, physics, chemistry, Math, English, Biology, History";
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("file://source"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoresFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write().to("file://destination")
                        .withHeader("Name, Total").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String>{
        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c){
            String row = c.element();

            if(!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class ComputeTotalScoresFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c){
            String[] data = c.element().split(",");
            String name = data[1];

            Integer totalScore = Integer.parseInt(data[2]) + Integer.parseInt(data[3]) +
                    Integer.parseInt(data[4]) + Integer.parseInt(data[5]) + Integer.parseInt(data[6])
                    + Integer.parseInt(data[7]);

            c.output(KV.of(name, totalScore));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Integer>, String> {

        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(c.element().getKey() + "," + c.element().getValue());
        }
    }
}
