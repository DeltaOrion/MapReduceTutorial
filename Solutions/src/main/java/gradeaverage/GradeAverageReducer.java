package gradeaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class GradeAverageReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text course, Iterator<DoubleWritable> iterator,
                       OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        //once again simply add up all of the keys associated
        double sum = 0;
        int n = 0;
        while(iterator.hasNext()) {
            sum += iterator.next().get();
            n++;
        }

        //divide the result to get the average
        outputCollector.collect(course,new DoubleWritable(sum/n));
    }
}
