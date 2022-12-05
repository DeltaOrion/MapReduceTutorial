package studentrank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class StudentRankMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private TreeSet<KeyValue> sorter;
    private final static int N = 10;

    //define the comma split regex (more efficient).
    private final static Pattern COMMA_SPLIT = Pattern.compile(",");
    private final static Pattern COLON_SPLIT = Pattern.compile(":");

    @Override
    public void setup(Context context) {
        //create the treeset, this may also take a comparator
        sorter = new TreeSet<>();
    }

    @Override
    public void map(Object o, Text input,
                    Context context) throws IOException, InterruptedException {

        String line = input.toString();
        //split the line by commas
        //ALTERNATIVE - String[] studentFields = line.split(",");
        String[] studentFields = COMMA_SPLIT.split(line);

        //courses start at the third comma separated value.
        int n = 0;
        double sum = 0;
        for(int i=3;i<studentFields.length;i++) {
            //create the key-value pair for the course
            //split the course by colon to separate the course from the grade.
            //alternative studentFields[i].split(":");
            String[] courseGrade = COLON_SPLIT.split(studentFields[i]);
            n++;
            sum += Double.parseDouble(courseGrade[1]);
            //collect the output to be sorted-shuffled and reduced.
        }

        KeyValue v = new KeyValue(studentFields[1],sum/n);
        //insert the item into the treemap so it can be sorted
        sorter.add(v);
        //remove the smallest item in the treeset if there are
        //more than 10 items.
        if(sorter.size()>N)
            sorter.pollLast();
    }

    @Override
    public void cleanup(Context context) throws IOException,InterruptedException {
        for(KeyValue value : sorter) {
            context.write(new Text(value.getKey()),new DoubleWritable(value.getValue()));
        }
    }


}
