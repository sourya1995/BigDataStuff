package Beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class StockPricePercentageDeltaComputation {
    private static final String CSV_HEADER = "Date, Open, High, Low, Close, Adj Close, Volume, Name";

    public interface ComputationOptions extends PipelineOptions {
        @Description("path of the file to read from")
        @Default.String("path/to/file")
        String getInputFile();

        void setInputFile(String value);
        @Description("path of the file to write to")
        @Validation.Required
        @Default.String("path/to/file")
        String getOutputFile();

        void setOutputFile(String value);

    }

    public static void main(String[] args) {
        ComputationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .watchForNewFiles(Duration.standardSeconds(10),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(30)))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))

                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        System.out.println(c.element());
                    }
                }));
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

            if(!row.isEmpty() && !row.equals(this.header)){
                c.output(row);
            }
        }
    }

    private static class ComputePriceDeltaPercentage extends DoFn<String, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] data = c.element().split(",");
            String date = data[0];

            double openPrice = Double.parseDouble(data[1]);
            double closePrice = Double.parseDouble(data[4]);

            double percentageDelta = ((closePrice - openPrice) / openPrice) * 100;

            Double percentageDeltaRounded = Math.round(percentageDelta * 100) / 100.0;
            c.output(KV.of(date, percentageDeltaRounded));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Double>, String> {
        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(c.element().getKey() + "," + c.element().getValue());
        }
    }
}
