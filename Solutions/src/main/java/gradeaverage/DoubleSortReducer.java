package gradeaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class DoubleSortReducer extends MapReduceBase implements Reducer<DoubleWritable, Text,Text,Text> {
    @Override
    public void reduce(DoubleWritable grade, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        //simply output each key value associated with the grade
        //note that there could be multiple courses with the same grade average
        while (iterator.hasNext()) {
            outputCollector.collect(iterator.next(),new Text(String.format("%.2f",grade.get())));
        }
    }
}
