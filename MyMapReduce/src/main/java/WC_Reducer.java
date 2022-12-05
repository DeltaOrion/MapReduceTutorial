import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WC_Reducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    /*
     * This method is called once for each individual key instance.
     * Each key can have multiple values.
     */
    public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
        int sum=0;
        //loop through all of the values associated with this key
        while (values.hasNext()) {
            //sum all of the values together to get the final count
            //of the amount of words of this key.
            sum+=values.next().get();
        }
        //write the final output out.
        output.collect(key,new IntWritable(sum));
    }
}
