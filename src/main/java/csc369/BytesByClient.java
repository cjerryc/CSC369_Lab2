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

public class BytesByClient {
    // ----- Job 1 -----
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;
    //    Mapper 1 to Count Bytes by Hostname/IPv4 Address (Mapper is called once every line of the input text file)
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable bytesWritable = new IntWritable();
        private final Text ipKey = new Text("10.0.0.153");  // hardcoded Client Hostname/IPv4 Address

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Using Apache log format: "IP - - [date] \"GET /path HTTP/1.1\" ..."
            String line = value.toString();
            String[] parts = line.split(" ");  // convert the line to a string and get different fields from Apache log by space separator
            if (parts.length > 9) {
                String ip = parts[0];          // Client Hostname/IPv4 Address
                String bytesStr = parts[9];    // Bytes-sent field

                // skip if "-" (some logs use "-" when no bytes sent)
                if (ip.equals("10.0.0.153") && !bytesStr.equals("-")) {
                    try {
                        int bytes = Integer.parseInt(bytesStr);
                        bytesWritable.set(bytes);
                        context.write(ipKey, bytesWritable);  // set (key, value) pair as (ip, Bytes)
                    } catch (NumberFormatException e) {
                        // ignore malformed byte count
                    }
                }
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

}

