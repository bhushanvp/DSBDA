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
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath = new Path(files[1]);

        Job job = Job.getInstance(conf, "TrackStatistics");
        job.setJarByClass(MusicProcessingApp2.class);
        job.setMapperClass(TrackMapper.class);
        job.setReducerClass(TrackReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TrackMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text trackId = new Text();
        private IntWritable radioCount = new IntWritable();
        private IntWritable skipCount = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                // Skip the CSV header row
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length >= 5) {
                String trackIdStr = fields[1].trim();
                int radio = Integer.parseInt(fields[3].trim());
                int skip = Integer.parseInt(fields[4].trim());

                trackId.set(trackIdStr);
                radioCount.set(radio);
                skipCount.set(skip);
               
                context.write(trackId, radioCount);
                context.write(trackId, skipCount);
            }
        }
    }

    public static class TrackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int radioCount = 0;
            int skipCount = 0;

            for (IntWritable value : values) {
                if (value.get() == 1) {
                    radioCount++;
                } else if (value.get() == 0) {
                    skipCount++;
                }
            }

            context.write(key, new IntWritable(radioCount));
            context.write(new Text("Total Radio Count"), new IntWritable(radioCount));
            context.write(new Text("Skip Count"), new IntWritable(skipCount));
        }
    }

}
