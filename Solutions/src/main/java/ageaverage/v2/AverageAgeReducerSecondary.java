package ageaverage.v2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class AverageAgeReducerSecondary extends MapReduceBase implements Reducer<IntWritable, Text,Text,Text> {

    @Override
    public void reduce(IntWritable key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        long sum = 0;
        long n = 0;

        while (iterator.hasNext()) {
            StringTokenizer tokenizer = new StringTokenizer(iterator.next().toString());
            long sumVal = Long.parseLong(tokenizer.nextToken());
            long nVal = Long.parseLong(tokenizer.nextToken());

            //there are n students of the age
            //so n increases by n and the sum increases by n*age
            n+= nVal;
            sum += (sumVal * nVal);
        }

        //output in %.2f
        outputCollector.collect(new Text("Average Age"),new Text(String.format("%.2f",sum/(double)n)));
    }
}
