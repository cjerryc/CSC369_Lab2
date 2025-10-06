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

public class RequestCountByMonthYear {
    // ----- Job 1 -----
    public static final Class OUTPUT_KEY_CLASS_1 = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_1 = IntWritable.class;

    //    Mapper 1 to Count Requests by MonthYear (Mapper is called once every line of the input text file)
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text monthYear = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Using Apache log format: "IP - - [date] \"GET /path HTTP/1.1\" httpRes bytes"
            String line = value.toString();
            String[] parts = line.split(" ");  // convert the line to a string and get different fields from Apache log by space separator
            if (parts.length > 3) {
                String Date = parts[3];  // parts[3] is the DateTime portion
                String[] DateSplit = Date.replace("[", "").split(":")[0].split("/");
                if (DateSplit.length >= 3) { // assemble the Date as YEAR MONTH to sort lexographically
                    String month = DateSplit[1];
                    String year = DateSplit[2];
                    monthYear.set(year + " " + monthToInt(month)); // e.g. "2004 03"
                    context.write(monthYear, one);
                }
            }
        }

        private String monthToInt(String month) {
            switch (month) {
                case "Jan":
                    return "01";
                case "Feb":
                    return "02";
                case "Mar":
                    return "03";
                case "Apr":
                    return "04";
                case "May":
                    return "05";
                case "Jun":
                    return "06";
                case "Jul":
                    return "07";
                case "Aug":
                    return "08";
                case "Sep":
                    return "09";
                case "Oct":
                    return "10";
                case "Nov":
                    return "11";
                case "Dec":
                    return "12";
                default:
                    return "0";
            }
        }
    }

    // Reducer 1 to Count Requests by MonthYear (Hadoop groups the output from Mapper by unique keys, so when the reducer is applied
    // to the data, the reduce method is called once on every unique key)
    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable total = new IntWritable();
        private Text monthYear = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) { // sum up counts of the monthYears
                sum += v.get();
            }
            total.set(sum);

            // reassemble the Date as MONTH YEAR again
            String[] DateSplit = key.toString().split(" ");
            String month = DateSplit[1];
            String year = DateSplit[0];
            monthYear.set(intToMonth(month) + " " + year);
            context.write(monthYear, total);
        }
        private String intToMonth(String month) {
            switch (month) {
                case "01":
                    return "Jan";
                case "02":
                    return "Feb";
                case "03":
                    return "Mar";
                case "04":
                    return "Apr";
                case "05":
                    return "May";
                case "06":
                    return "Jun";
                case "07":
                    return "Jul";
                case "08":
                    return "Aug";
                case "09":
                    return "Sep";
                case "10":
                    return "Oct";
                case "11":
                    return "Nov";
                case "12":
                    return "Dec";
                default:
                    return "0";
            }
        }
    }
}

