import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // We can ignore the key and only work with value
        String[] words = value.toString().split(" ");
        
        System.out.println(key + " " + value);
        for (String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}