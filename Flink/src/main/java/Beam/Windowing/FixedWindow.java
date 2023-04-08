package Beam.Windowing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class FixedWindow {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> carMakesTimes = pipeline.apply(Create.timestamped(
                TimestampedValue.of(("Ford"), new DateTime("2020-12-12T20:30:05").toInstant())
        ));

        PCollection<String> windowedMakesTimes = carMakesTimes.apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(5))));
        PCollection<KV<String, Long>> output = windowedMakesTimes.apply(Count.perElement());
        output.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
                System.out.println(String.format("%s %s %s", window.maxTimestamp(), c.element().getKey(), c.element().getValue()));
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
