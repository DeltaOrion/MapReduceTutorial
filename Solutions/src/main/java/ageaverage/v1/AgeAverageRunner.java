package ageaverage.v1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class AgeAverageRunner {

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(AgeAverageRunner.class);
        conf.setJobName("Average Age");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        conf.setCombinerClass(AgeAverageReducer.class);
        //define output keys,values
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        //Define the mapper and reducer
        conf.setMapperClass(AgeAverageMapper.class);
        conf.setReducerClass(AgeAverageReducer.class);

        //define how the data will be inputted and outputted
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        //Run and Time the job. We can simply run it but lets
        //time it for fun.
        System.out.println("Starting MapReduce Average Age Job");
        long start = System.currentTimeMillis();

        //Finally run the job
        JobClient.runJob(conf);

        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");

    }
}
