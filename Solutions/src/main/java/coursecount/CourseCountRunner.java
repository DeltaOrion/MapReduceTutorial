package coursecount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class CourseCountRunner {

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(CourseCountRunner.class);
        conf.setJobName("Course Count");

        //define output keys,values
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //Define the mapper and reducer
        conf.setMapperClass(CourseCountMapper.class);
        conf.setReducerClass(CourseCountReducer.class);

        //We can safely define this combiner class
        //as we are simply adding the values. No other math is being
        //done
        conf.setCombinerClass(CourseCountReducer.class);

        //define how the data will be inputted and outputted
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        //Run and Time the job. We can simply run it but lets
        //time it for fun.
        System.out.println("Starting MapReduce Course Count Job");
        long start = System.currentTimeMillis();

        //Finally run the job
        JobClient.runJob(conf);

        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");

    }
}
