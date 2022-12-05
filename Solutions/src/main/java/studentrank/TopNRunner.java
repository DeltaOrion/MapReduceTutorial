package studentrank;

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

public class TopNRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job conf = Job.getInstance(configuration);
        conf.setJarByClass(TopNRunner.class);
        conf.setJobName("Student Rank");

        //define output keys,values
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        //Define the mapper and reducer
        conf.setMapperClass(TopNMapper.class);
        conf.setReducerClass(TopNReducer.class);

        //We can safely define this combiner class
        //as we are simply ordering values.
        //conf.setCombinerClass(TopNReducer.class);

        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Finally run the job
        boolean success = conf.waitForCompletion(false);
    }
}
