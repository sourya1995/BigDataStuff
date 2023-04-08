package Beam.Windowing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class FixedWindowEnergy {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<EnergyConsumption> energyConsumption = pipeline
                .apply("ReadEnergyConsumption",
                        TextIO.read().from("file/to/path"))
                .apply("ParseEnergyData",
                        ParDo.of(new ParseEnergyDataFn()))
                .apply(WithTimestamps.of(EnergyConsumption::getDatetime));

        energyConsumption.apply("Window",Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply("ToStrings",MapElements
                        .into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRow(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("file/path").withSuffix(".csv")
                        .withHeader(EnergyConsumption.getCSVHeader())
                        .withNumShards(1)
                        .withWindowedWrites());

        pipeline.run().waitUntilFinish();
    }

    private static class ParseEnergyDataFn extends DoFn<String, EnergyConsumption> {
        private static final String[] FILE_HEADER_MAPPING = {
                "Datetime", "AEP_MW"
        };

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final CSVParser parser = new CSVParser(new StringReader(
                    c.element()),
                    CSVFormat.DEFAULT
                            .withDelimiter(',')
                            .withHeader(FILE_HEADER_MAPPING));
            CSVRecord record = parser.getRecords().get(0);

            if(record.get("Datetime").contains("Datetime")) {
                return;
            }

            DateTimeZone timeZone = DateTimeZone.forID("Asia/Kolkata");
            DateTime date = LocalDateTime.parse(record.get("Datetime").trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDateTime(timeZone);

            EnergyConsumption consumption = new EnergyConsumption();
            consumption.setDatetime(date.toInstant());
            consumption.getEnergyConsumption(Double.valueOf(record.get("AEP_MW")));
            c.output(consumption);

        }
    }
}
