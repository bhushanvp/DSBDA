import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieRecommendationApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path inputPath = new Path(files[0]);
        Path outputPath = new Path(files[1]);

        // Set the target user and the movie to recommend
        String targetUser = "1";
        String movieToRecommend = "31";

        configuration.set("targetUser", targetUser);
        configuration.set("movieToRecommend", movieToRecommend);

        Job job = new Job(configuration, "Movie Recommendation");
        job.setJarByClass(MovieRecommendationApp.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text movieDetails = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            String userIdValue = fields[0];
            String movieId = fields[1];
            String rating = fields[2];

            userId.set(userIdValue);
            movieDetails.set(movieId + "," + rating);
            context.write(userId, movieDetails);
        }
    }

    public static class MovieReducer extends Reducer<Text, Text, Text, Text> {
        private String targetUser;
        private String movieToRecommend;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            targetUser = configuration.get("targetUser");
            movieToRecommend = configuration.get("movieToRecommend");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(targetUser)) {
                for (Text value : values) {
                    String[] movieDetails = value.toString().split(",");
                    String movieId = movieDetails[0];
                    String rating = movieDetails[1];
                    if (movieId.equals(movieToRecommend)) {
                        context.write(new Text("User ID: " + targetUser), new Text("Recommended Movie: " + movieToRecommend + ", Rating: " + rating));
                        return;
                    }
                }
            }
        }
    }
}
