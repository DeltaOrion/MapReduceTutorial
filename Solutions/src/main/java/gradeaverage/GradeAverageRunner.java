package gradeaverage;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class GradeAverageRunner {

    public static void main(String[] args) throws IOException {

        //get input and output from args
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        //define the intermediate output, i.e.e the output of the first job
        //and the input of the second job
        Path intermediateOut = new Path(output,"temp");
        //define the final output folder of the second mapreduce job.
        Path finalOut = new Path(output,"final");

        GradeAverageRunner runner = new GradeAverageRunner();

        runner.calculateAverage(input,intermediateOut);
        runner.sortOutput(intermediateOut,finalOut);
    }

    public void calculateAverage(Path input, Path output) throws IOException {
        JobConf averageJob = new JobConf(GradeAverageRunner.class);
        averageJob.setJobName("Grade Average");

        //define input keys,values
        averageJob.setMapOutputKeyClass(Text.class);
        averageJob.setMapOutputValueClass(DoubleWritable.class);

        //define output keys,values
        averageJob.setOutputKeyClass(Text.class);
        averageJob.setOutputValueClass(DoubleWritable.class);

        //Define the mapper and reducer
        averageJob.setMapperClass(GradeAverageMapper.class);
        //we have to define this as a double
        averageJob.setReducerClass(GradeAverageReducer.class);

        //Note that we have removed the combiner class
        //to actually use a combiner we would need to create a custom
        //one that would add the values and keep track of the amount
        //of values added in the combine stage

        //define how the data will be inputted and outputted
        averageJob.setInputFormat(TextInputFormat.class);
        averageJob.setOutputFormat(TextOutputFormat.class);

        //define where to output and input the data
        FileInputFormat.setInputPaths(averageJob,input);
        FileOutputFormat.setOutputPath(averageJob,output);

        //Run and Time the job. We can simply run it but lets
        //time it for fun.
        System.out.println("Starting MapReduce Grade Average Job");
        long start = System.currentTimeMillis();

        //Finally run the job
        JobClient.runJob(averageJob);

        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");
    }

    public void sortOutput(Path input, Path output) throws IOException {
        JobConf sortJob = new JobConf(GradeAverageRunner.class);

        //Sorting the output
        System.out.println("Sorting Output");
        sortJob.setMapperClass(DoubleSortMapper.class);
        sortJob.setReducerClass(DoubleSortReducer.class);

        //define classes
        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        //output must be TEXT as we need to convert
        //the double to %.2f before outputting
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setOutputKeyComparatorClass(DescendingComparator.class);

        sortJob.setInputFormat(TextInputFormat.class);
        sortJob.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(sortJob, input);
        FileOutputFormat.setOutputPath(sortJob, output);

        JobClient.runJob(sortJob);
        System.out.println("Finished Sorting Output");
    }

    /**
     * Define our custom comparator which is used by hadoop to automatically
     * sort the doubles in descending orders
     */
    private static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            //read the two values
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            //reverse double.compare
            return Double.compare(thatValue, thisValue);
        }
    }
}
