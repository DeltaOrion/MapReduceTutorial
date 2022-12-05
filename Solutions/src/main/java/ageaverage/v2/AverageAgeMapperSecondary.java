package ageaverage.v2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Pattern;

public class AverageAgeMapperSecondary extends MapReduceBase implements Mapper<Object, Text,IntWritable, Text> {

    private final IntWritable KEY = new IntWritable(1);

    @Override
    public void map(Object obj, Text avgData, OutputCollector<IntWritable, Text> outputCollector,
                    Reporter reporter) throws IOException {
        //simply pass the data along to the reducer for it to calculate the average
        //funnel all of the data to a single key.
        outputCollector.collect(KEY, avgData);
    }
}
