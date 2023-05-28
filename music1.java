import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MusicProcessingApp1 {

  public static class ListenerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      String userId = fields[0];
      int shared = Integer.parseInt(fields[2]);

      context.write(new Text(userId), new IntWritable(shared));
    }
  }

  public static class ListenerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int uniqueListeners = 0;
      int trackShares = 0;

      for (IntWritable value : values) {
        uniqueListeners++;
        trackShares += value.get();
      }

      context.write(new Text("Unique Listeners"), new IntWritable(uniqueListeners));
      context.write(new Text("Track Shares"), new IntWritable(trackShares));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unique Listeners and Track Shares");
    job.setJarByClass(MusicProcessingApp1.class);
    job.setMapperClass(ListenerMapper.class);
    job.setReducerClass(ListenerReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
 
