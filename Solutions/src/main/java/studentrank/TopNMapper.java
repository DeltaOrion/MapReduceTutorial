package studentrank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

public class TopNMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    /*
     * Alternative
     *  - Using a priority queue
     */
    private TreeSet<KeyValue> sorter;
    private final static int N = 10;

    @Override
    public void setup(Context context) {
        //create the treeset, this may also take a comparator
        sorter = new TreeSet<>();
    }

    @Override
    public void map(Object o, Text input,
                Context context) throws IOException, InterruptedException {
        String key = "";
        double value = 0;
        /*
         * TODO
         *  - Process input to produce key value pair
         */

        KeyValue v = new KeyValue(key, value);
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
