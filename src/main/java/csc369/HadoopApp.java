package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.out.println("Expected parameters: <job class> <input dir> <output dir>");
            System.exit(-1);
        } else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(WordCount.ReducerImpl.class);
            job.setMapperClass(WordCount.MapperImpl.class);
            job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
        } else if ("RequestCountByURL".equalsIgnoreCase(otherArgs[0])) {
            // ----- Job 1: Count requests per URL -----
            job.setMapperClass(RequestCountByURL.Mapper1.class);
            job.setReducerClass(RequestCountByURL.Reducer1.class);
            job.setOutputKeyClass(RequestCountByURL.OUTPUT_KEY_CLASS_1);
            job.setOutputValueClass(RequestCountByURL.OUTPUT_VALUE_CLASS_1);

            // Set the input dir and output dir to print report to.
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            Path tempOutput = new Path("temp_requestcount_output");
            FileOutputFormat.setOutputPath(job, tempOutput);

            job.waitForCompletion(true);

            // ----- Job 2: Sort by request count -----
            Job job2 = new Job(conf, "Sort by Count");
            job2.setMapperClass(RequestCountByURL.Mapper2.class);
            job2.setReducerClass(RequestCountByURL.Reducer2.class);
            job2.setOutputKeyClass(RequestCountByURL.OUTPUT_KEY_CLASS_2);
            job2.setOutputValueClass(RequestCountByURL.OUTPUT_VALUE_CLASS_2);

            // Set the input dir and output dir to print report to. Exit out of program after job completion.
            FileInputFormat.addInputPath(job2, tempOutput);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.out.println("Unrecognized job: " + otherArgs[0]);
            System.exit(-1);
        }

        // Set the input dir and output dir to print report to. Exit out of program after job completion.
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
