import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class WC_Runner {
    public static void main(String[] args) throws IOException{
        JobConf conf = new JobConf(WC_Runner.class);
        conf.setJobName("WordCount");

        //define mapper output keys, values
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        //define output keys,values
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //Define the mapper and reducer
        conf.setMapperClass(WC_Mapper.class);
        conf.setReducerClass(WC_Reducer.class);

        //define the combiner class, this will sum the output
        //of the mapper. Remember mapping instances run in parallel so this will only
        //be the local sum. The final sum must be processed by the reducer.
        conf.setCombinerClass(WC_Reducer.class);

        //define how the data will be inputted and outputted
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        //Run and Time the job
        System.out.println("Starting MapReduce Word Count Job");
        long start = System.currentTimeMillis();

        //Finally run the job
        JobClient.runJob(conf);
        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");
    }
}
