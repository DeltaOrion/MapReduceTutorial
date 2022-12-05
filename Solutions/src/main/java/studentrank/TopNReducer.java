package studentrank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class TopNReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private TreeSet<KeyValue> sorter;
    private final static int N = 10;

    @Override
    public void setup(Context context) {
        sorter = new TreeSet<>();
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //add all of the key value pairs to the sorter
        for(DoubleWritable value : values) {
            //add the key value pair to the set to be sorted
            sorter.add(new KeyValue(key.toString(),value.get()));
            //if we have more than 10, remove the last entry
            if(sorter.size()>N) {
                sorter.pollLast();
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        //add the top 10 entries in!
        for(KeyValue value : sorter) {
            context.write(new Text(value.getKey()),new DoubleWritable(value.getValue()));
        }
    }
}
