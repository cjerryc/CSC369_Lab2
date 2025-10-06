package csc369;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BytesByDay {
    // ----- Job 1 -----
    public static final Class OUTPUT_KEY_CLASS_1 = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_1 = IntWritable.class;
    //    Mapper 1 to Count Bytes by Hostname/IPv4 Address (Mapper is called once every line of the input text file)
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable bytesWritable = new IntWritable();
        private Text Day = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Using Apache log format: "IP - - [date] \"GET /path HTTP/1.1\" httpRes bytes"
            String line = value.toString();
            String[] parts = line.split(" ");  // convert the line to a string and get different fields from Apache log by space separator
            if (parts.length > 9) {
                String bytesStr = parts[9];    // Bytes-sent field
                String Date = parts[3];  // parts[3] is the DateTime portion
                String[] DateSplit = Date.replace("[", "").split(":")[0].split("/");
                if (DateSplit.length >= 3) { // assemble the Date as YEAR MONTH to sort lexographically
                    String day = DateSplit[0];
                    String month = DateSplit[1];
                    String year = DateSplit[2];
                    Day.set(month + " " + day + " " + year); // e.g. "Mar 15 2004"
                }
                int bytes = Integer.parseInt(bytesStr);
                bytesWritable.set(bytes);
                context.write(Day, bytesWritable);  // set (key, value) pair as (Day, Bytes)
            }
        }
    }
    // Reducer 1 to Count Requests by Hostname/IPv4 Address (Hadoop groups the output from Mapper by unique keys, so when the reducer is applied
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
            context.write(key, total); // output: (10.0.0.153, totalBytes)
        }
    }

    // ----- Job 2 -----
    public static final Class OUTPUT_KEY_CLASS_2 = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS_2 = Text.class;
    //    Mapper 2 to sort by total Bytes in ASCENDING order
    public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable bytesWritable = new IntWritable();  // Hadoop variables to reuse for efficiency
        private Text Date = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input line: "/index.html    10"
            String[] parts = value.toString().split("\\s+");  //grab text input from input text file
            if (parts.length == 4) {
                String month = parts[0];
                String day = parts[1];
                String year = parts[2];
                Date.set(month + " " + day + " " + year); // e.g. "Mar 15 2004"

                String bytesStr = parts[3]; // get bytes
                int bytes = Integer.parseInt(bytesStr);
                bytesWritable.set(bytes);
                context.write(bytesWritable, Date);  // set (key, value) pair as (Day, Bytes)
            }
        }
    }
    //    Reducer 2 to sort by total Bytes in ASCENDING order
    public static class Reducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values)
                context.write(key, v);  // (count, HTTP response code)
        }
    }

}

