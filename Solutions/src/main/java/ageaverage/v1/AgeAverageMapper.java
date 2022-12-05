package ageaverage.v1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Pattern;

public class AgeAverageMapper extends MapReduceBase implements Mapper<Object, Text,Text, DoubleWritable> {

    //define the comma split regex (more efficient).
    private final static Pattern COMMA_SPLIT = Pattern.compile(",");
    private final Text KEY = new Text("Average Age");

    @Override
    public void map(Object obj, Text text, OutputCollector<Text, DoubleWritable> outputCollector,
                    Reporter reporter) throws IOException {

        String line = text.toString();
        //split the line by commas
        //ALTERNATIVE - String[] studentFields = line.split(",");
        String[] studentFields = COMMA_SPLIT.split(line);

        //simply funnel all of the student ages to a single key so
        //that the reducer adds them up and averages them
        outputCollector.collect(KEY,new DoubleWritable(Double.parseDouble(studentFields[2])));

    }
}
