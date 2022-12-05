package coursecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CourseCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    public void reduce(Text course, Iterator<IntWritable> iterator,
                       OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        //add up all of the (amount of people) doing each course
        int sum = 0;
        while(iterator.hasNext()) {
            sum += iterator.next().get();
        }

        outputCollector.collect(course,new IntWritable(sum));
    }
}
