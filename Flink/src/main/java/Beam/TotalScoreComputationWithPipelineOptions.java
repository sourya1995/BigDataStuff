package Beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputationWithPipelineOptions {
    private static final String CSV_HEADER = "id, name, physics, chemistry, Math, English, Biology, History";

    public interface TotalScoreComputationOptions extends PipelineOptions {
        @Description("path of the file to read from")
        @Default.String("path/to/file")
        String getInputFile();

        void setInputFile(String value);
        @Description("path of the file to write to")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        System.out.println("Input file: " + options.getInputFile());
        System.out.println("Output file: " + options.getOutputFile());

        pipeline.apply(TextIO.read().from("file://source"))
                .apply(ParDo.of(new TotalScoreComputation.FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new TotalScoreComputation.ComputeTotalScoresFn()))
                .apply(ParDo.of(new TotalScoreComputation.ConvertToStringFn()))
                .apply(TextIO.write().to("file://destination")
                        .withHeader("Name, Total").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {
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
