import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    /**
     * this method will be fed the input line by line by the
     * TextInputFormat.java as seen in WC_Runner.java. This means
     * that the method is called each time a new line needs to be processed
     */
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
        //convert the line to a string so its easier to process
        String line = value.toString();
        //split the line by spaces
        //for example "abc abc abc" -> "abc","abc","abc"
        StringTokenizer tokenizer = new StringTokenizer(line);
        //loop through each word
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            //create a key value pair with the word and the value 1
            //for example - word: 1
            //and save the output of the mapping task
            output.collect(word, one);
        }
    }

}
