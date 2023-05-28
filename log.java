import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogFileProcessingApp {
    public static void main(String [] args) throws Exception{
                Configuration c = new Configuration();
                String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
                Path input=new Path(files[0]);
                Path output=new Path(files[1]);
                Job j=new Job(c,"LogFileProcessing");
                j.setJarByClass(LogFileProcessingApp.class);
                j.setMapperClass(MapForWordCount.class);
                j.setReducerClass(ReduceForWordCount.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(j, input);
                FileOutputFormat.setOutputPath(j, output);
                System.exit(j.waitForCompletion(true)?0:1);
        }

    public static class MapForWordCount extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String[] fields = value.toString().split(",");
            
            // Extract relevant fields from the CSV line
            String userID = fields[0];
            String loginTime = fields[5];
            String logoutTime = fields[7];
            
            // Emit the userID as the output key and the duration as the output value
            context.write(new Text(userID), new Text(loginTime + "," + logoutTime));
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long maxDuration = 0;
            String maxDurationInfo = "";
            // Iterate through the durations to find the maximum
            
            for (Text value : values) {
                String[] timestamps = value.toString().split(",");
                long loginTimestamp = Timestamp.valueOf(timestamps[0]).getTime();
                long logoutTimestamp = Timestamp.valueOf(timestamps[1]).getTime();
                long duration = logoutTimestamp - loginTimestamp;
        
                if (duration > maxDuration) {
                    maxDuration = duration;
                    maxDurationInfo = timestamps[0] + "," + timestamps[1];
                }
            }
        
            // Output the user ID(s) with the maximum duration
            context.write(key, new Text(maxDurationInfo));
        }
    }
} 
