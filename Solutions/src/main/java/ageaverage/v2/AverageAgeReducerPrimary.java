package ageaverage.v2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class AverageAgeReducerPrimary extends MapReduceBase implements Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {


    @Override
    public void reduce(LongWritable longWritable, Iterator<LongWritable> iterator, OutputCollector<LongWritable, LongWritable> outputCollector, Reporter reporter) throws IOException {

        //add up all of the students with the given age
        long sum = longWritable.get();
        long n = 0;
        while (iterator.hasNext()) {
            n += iterator.next().get();
        }

        //output this to the final file.
        outputCollector.collect(new LongWritable(sum),new LongWritable(n));
    }
}
