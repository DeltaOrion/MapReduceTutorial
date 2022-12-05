package gradeaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class DoubleSortMapper extends MapReduceBase implements Mapper<Text, DoubleWritable, DoubleWritable, Text> {

    @Override
    public void map(Text key, DoubleWritable output, OutputCollector<DoubleWritable, Text> outputCollector, Reporter reporter) throws IOException {
        //the first map reduce job outputted
        //key   double
        //split this
        String line = key.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        Text course = new Text(tokenizer.nextToken());
        DoubleWritable number = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));
        //it it important to note that the DOUBLE is first
        //because hadoop automatically sorts by KEYS
        outputCollector.collect(number,course);
    }
}
