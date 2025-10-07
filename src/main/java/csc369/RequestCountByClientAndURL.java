package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestCountByClientAndURL {
    // ----- Job 1 -----
    public static final Class OUTPUT_KEY_CLASS_1 = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_1 = IntWritable.class;
    //    Mapper 1 to Count Requests by Client for a Specific URl (Mapper is called once every line of the input text file)
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text Client = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Using Apache log format: "IP - - [date] \"GET /path HTTP/1.1\" httpRes bytes"
            String line = value.toString();
            String[] parts = line.split(" ");  // convert the line to a string and get different fields from Apache log by space separator
            if (parts.length > 6) {
                String URL = parts[6];  // parts[6] is the URL portion
                if (URL.equals("/twiki/bin/view/Main/WebHome")){
                    String ip = parts[0];          // Client Hostname/IPv4 Address
                    Client.set(ip);
                    context.write(Client, one);  // set (key, value) pair as (Hostname/IPv4 Address, count)
                }
            }
        }
    }
    // Reducer 1 to Count Requests by Client for a Specific URl (Hadoop groups the output from Mapper by unique keys, so when the reducer is applied
    // to the data, the reduce method is called once on every unique key)
    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values){
                sum += v.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    // ----- Job 2 -----
    public static final Class OUTPUT_KEY_CLASS_2 = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS_2 = Text.class;
    //    Mapper 2 to sort by request count in ASCENDING order
    public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();  // Hadoop variables to reuse for efficiency
        private Text Client = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input line: "/index.html    10"
            String[] parts = value.toString().split("\\s+");  //grab text input from input text file
            if (parts.length == 2) {
                Client.set(parts[0]);
                count.set(Integer.parseInt(parts[1]));
                context.write(count, Client);  // set (key, value) pair as (count, Hostname/IPv4 Address)
            }
        }
    }
    //    Reducer 2 to sort by request count in ASCENDING order
    public static class Reducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values)
                context.write(key, v);  // (count, Hostname/IPv4 Address)
        }
    }

}

