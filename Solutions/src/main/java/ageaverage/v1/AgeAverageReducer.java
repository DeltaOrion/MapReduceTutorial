package ageaverage.v1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class AgeAverageReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> iterator,
                       OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {

        //calculate the average of all of the keys
        double sum = 0;
        int n = 0;
        while(iterator.hasNext()) {
            sum += iterator.next().get();
            n++;
        }

        outputCollector.collect(key,new DoubleWritable(sum/n));
    }
}
