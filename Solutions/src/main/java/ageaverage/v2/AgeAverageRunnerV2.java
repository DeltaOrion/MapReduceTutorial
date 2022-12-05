package ageaverage.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class AgeAverageRunnerV2 {

    public static void main(String[] args) throws IOException {
        Path output = new Path(args[1]);

        JobConf conf = new JobConf(AgeAverageRunnerV2.class);
        conf.setJobName("Average Age V2");

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(LongWritable.class);

        conf.setCombinerClass(AverageAgeReducerPrimary.class);
        //define output keys,values
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setCombinerClass(AverageAgeReducerPrimary.class);

        //Define the mapper and reducer
        conf.setMapperClass(AverageAgeMapperPrimary.class);
        conf.setReducerClass(AverageAgeReducerPrimary.class);

        //define how the data will be inputted and outputted
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(output,"temp"));

        //Run and Time the job. We can simply run it but lets
        //time it for fun.
        System.out.println("Count Ages");
        long start = System.currentTimeMillis();

        //Finally run the job
        JobClient.runJob(conf);
        System.out.println("Finished Counting Ages");

        JobConf average = new JobConf(AgeAverageRunnerV2.class);
        average.setJobName("Average Age V2");

        average.setMapOutputKeyClass(IntWritable.class);
        average.setMapOutputValueClass(Text.class);

        //define output keys,values
        average.setOutputKeyClass(Text.class);
        average.setOutputValueClass(Text.class);

        //Define the mapper and reducer
        average.setMapperClass(AverageAgeMapperSecondary.class);
        average.setReducerClass(AverageAgeReducerSecondary.class);

        //define how the data will be inputted and outputted
        average.setInputFormat(TextInputFormat.class);
        average.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(average,new Path(output,"temp"));
        FileOutputFormat.setOutputPath(average,new Path(output,"final"));
        System.out.println("Averaging values");

        //Finally run the job
        JobClient.runJob(average);

        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");
        System.out.println("Finished Averaging - Output written");

    }
}
