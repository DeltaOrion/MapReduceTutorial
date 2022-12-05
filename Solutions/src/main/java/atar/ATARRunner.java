package atar;

import gradeaverage.GradeAverageRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ATARRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        ATARRunner runner = new ATARRunner();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        runner.calculateATARs(input,output);
    }

    public ATARRunner() {
    }

    public void calculateATARs(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException {
        //calculate grade averages
        Path gradeOut = new Path(output,"grades");
        GradeAverageRunner gradeAvg = new GradeAverageRunner();
        gradeAvg.calculateAverage(input,gradeOut);

        //calculate the ATAR using the averages.
        Path ATAROut = new Path(output,"ATARS");
        processAtars(input,ATAROut);
    }

    private void processAtars(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job conf = Job.getInstance(configuration);
        conf.setJarByClass(ATARRunner.class);
        conf.setJobName("ATAR Job");

        //define output keys,values
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        //Define the mapper and reducer
        conf.setMapperClass(ATARMapper.class);
        conf.setReducerClass(ATARReducer.class);

        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.addInputPath(conf, input);
        FileOutputFormat.setOutputPath(conf, output);

        //Finally run the job
        System.out.println("Starting ATAR Job");
        long before = System.currentTimeMillis();
        boolean success = conf.waitForCompletion(true);
        long end = System.currentTimeMillis();
        if(success) {
            System.out.println("Finished ATAR Job");
        } else {
            System.out.println("ATAR Job Failed");
        }
    }


}
