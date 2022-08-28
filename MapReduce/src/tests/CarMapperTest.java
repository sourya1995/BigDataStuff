import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class TestCarMapper {

    public static void main(String[] args) throws IOException {
        (new TestCarMapper()).testMapper();
    }

    @Test
    public void testMapper() throws IOException {

        MapDriver<LongWritable, Text, Text, IntWritable> driver =
                MapDriver.<LongWritable, Text, Text, IntWritable>newMapDriver()
                        .withMapper(new CarMapper())
                        .withInput(new LongWritable(0), new Text("BMW BMW Toyota"))
                        .withInput(new LongWritable(0), new Text("Rolls-Royce Honda Honda"))
                        .withOutput(new Text("bmw"), new IntWritable(1))
                        .withOutput(new Text("bmw"), new IntWritable(1))
                        .withOutput(new Text("toyota"), new IntWritable(1))
                        .withOutput(new Text("rolls-royce"), new IntWritable(1))
                        .withOutput(new Text("honda"), new IntWritable(1))
                        .withOutput(new Text("honda"), new IntWritable(1));

        driver.runTest();
    }
}