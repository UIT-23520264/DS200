import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAvgByTitle {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text movieId = new Text();
        private final DoubleWritable ratingVal = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split(",\\s*");
            if (p.length < 3) return;

            movieId.set(p[1].trim());
            ratingVal.set(Double.parseDouble(p[2].trim()));
            context.write(movieId, ratingVal);
        }
    }

    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Map<String, String> movieMap = new HashMap<>();
        private String bestMovie = null;
        private double bestAvg = Double.NEGATIVE_INFINITY;

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                File f = new File(new File(uri.getPath()).getName());
                if (!f.exists()) continue;

                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] p = line.split(",\\s*", 3);
                        if (p.length >= 2) {
                            movieMap.put(p[0].trim(), p[1].trim());
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable v : values) {
                sum += v.get();
                count++;
            }

            double avg = sum / count;
            String movieId = key.toString();
            String title = movieMap.getOrDefault(movieId, movieId);

            context.write(new Text(title),
                    new Text(String.format("%.2f (TotalRatings: %d)", avg, count)));

            if (count >= 5 && avg > bestAvg) {
                bestAvg = avg;
                bestMovie = title;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (bestMovie == null) {
                context.write(new Text("TOP_MOVIE"),
                        new Text("No movie has at least 5 ratings."));
            } else {
                context.write(new Text("TOP_MOVIE"),
                        new Text(String.format("%s (Average: %.2f)", bestMovie, bestAvg)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = ratings_1.txt
        // args[1] = ratings_2.txt
        // args[2] = movies.txt
        // args[3] = output
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Avg By Title");
        job.setJarByClass(MovieAvgByTitle.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        job.setReducerClass(AvgReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}