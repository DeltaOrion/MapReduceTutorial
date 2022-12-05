package ageaverage.v2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Array;
import java.util.regex.Pattern;

public class AverageAgeMapperPrimary extends MapReduceBase implements Mapper<Object, Text, LongWritable,LongWritable> {

    //define the comma split regex (more efficient).
    private final static Pattern COMMA_SPLIT = Pattern.compile(",");
    private final Text KEY = new Text("Average Age");

    @Override
    public void map(Object obj, Text text, OutputCollector<LongWritable,LongWritable> outputCollector,
                    Reporter reporter) throws IOException {

        String line = text.toString();
        //split the line by commas
        //ALTERNATIVE - String[] studentFields = line.split(",");
        String[] studentFields = COMMA_SPLIT.split(line);
        int age = Integer.parseInt(studentFields[2]);

        //count the amount of students are of the current age
        outputCollector.collect(new LongWritable(age),new LongWritable(1));

    }
}
