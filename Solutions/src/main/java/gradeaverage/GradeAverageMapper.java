package gradeaverage;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Pattern;

public class GradeAverageMapper extends MapReduceBase implements Mapper<Object, Text,Text, DoubleWritable> {

    //define the comma split regex (more efficient).
    private final static Pattern COMMA_SPLIT = Pattern.compile(",");
    private final static Pattern COLON_SPLIT = Pattern.compile(":");

    @Override
    public void map(Object obj, Text input, OutputCollector<Text, DoubleWritable> outputCollector,
                    Reporter reporter) throws IOException {

        String line = input.toString();
        //split the line by commas
        //ALTERNATIVE - String[] studentFields = line.split(",");
        String[] studentFields = COMMA_SPLIT.split(line);

        //courses start at the third comma separated value.
        for(int i=3;i<studentFields.length;i++) {
            //create the key-value pair for the course
            //split the course by colon to separate the course from the grade.
            //alternative studentFields[i].split(":");
            String[] courseGrade = COLON_SPLIT.split(studentFields[i]);

            Text word = new Text();
            DoubleWritable count = new DoubleWritable(Double.parseDouble(courseGrade[1]));
            word.set(courseGrade[0]);

            //collect the output to be sorted-shuffled and reduced.
            outputCollector.collect(word,count);
        }

    }
}
