import java.io.IOException;
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

public class MusicProcessingApp2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath = new Path(files[1]);

        Job job = new Job(configuration, "Music Processing");
        job.setJarByClass(MusicProcessingApp2.class);
        job.setMapperClass(MusicMapper.class);
        job.setReducerClass(MusicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MusicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text trackId = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input CSV line by comma
            String[] fields = value.toString().split(",");
            
            // Extract the relevant fields
            String userId = fields[0];
            String trackIdValue = fields[1];
            int shared = Integer.parseInt(fields[2]);
            
            // Emit the userId as the key and 1 as the value
            context.write(new Text(userId), one);
            
            // Emit the trackId and shared count as the key and 1 as the value
            if (shared == 1) {
                trackId.set(new Text(trackIdValue));
                context.write(trackId, one);
            }
        }
    }

    public static class MusicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
 
