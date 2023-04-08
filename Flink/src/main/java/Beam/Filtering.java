package Beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.Arrays;
import java.util.List;

public class Filtering {
    public static class FilterThresholdFn extends DoFn<Double, Double> {
        private double threshold = 0;

        public FilterThresholdFn(double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c){
            if(c.element() > threshold){
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> googStockPrices = Arrays.asList(1367.56, 1360.66, 1394.20, 1393.33, 1401.31, 1419.82, 1429.73);

        pipeline.apply(Create.of(googStockPrices))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input){
                        System.out.println("Pre-filtered" + input);
                        return input;
                    }
                }))
                        .apply(ParDo.of(new FilterThresholdFn(1400)))
                                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                                    @Override
                                    public Void apply(Double input){
                                        System.out.println("Post-filtered" + input);
                                        return null;
                                    }
                                }));

        pipeline.run();
    }
}
