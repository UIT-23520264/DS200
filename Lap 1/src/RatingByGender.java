import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingByGender {

    public static class GenderRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> userGender = new HashMap<>();
        private final Text movieId = new Text();
        private final Text outVal = new Text();

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
                        String[] p = line.split(",\\s*");
                        if (p.length >= 2) {
                            userGender.put(p[0].trim(), p[1].trim());
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split(",\\s*");
            if (p.length < 3) return;

            String gender = userGender.get(p[0].trim());
            if (gender == null) return;

            movieId.set(p[1].trim());
            outVal.set(gender + ":" + p[2].trim());
            context.write(movieId, outVal);
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, String> movieTitles = new HashMap<>();

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
                            movieTitles.put(p[0].trim(), p[1].trim());
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double maleSum = 0.0, femaleSum = 0.0;
            int maleCount = 0, femaleCount = 0;

            for (Text t : values) {
                String[] p = t.toString().split(":");
                if (p.length < 2) continue;

                String g = p[0];
                double r = Double.parseDouble(p[1]);

                if ("M".equalsIgnoreCase(g)) {
                    maleSum += r;
                    maleCount++;
                } else if ("F".equalsIgnoreCase(g)) {
                    femaleSum += r;
                    femaleCount++;
                }
            }

            String title = movieTitles.getOrDefault(key.toString(), key.toString());
            double maleAvg = maleCount == 0 ? 0.0 : maleSum / maleCount;
            double femaleAvg = femaleCount == 0 ? 0.0 : femaleSum / femaleCount;

            context.write(new Text(title),
                    new Text(String.format("Male_Avg: %.2f, Female_Avg: %.2f", maleAvg, femaleAvg)));
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = ratings_1.txt
        // args[1] = ratings_2.txt
        // args[2] = users.txt
        // args[3] = movies.txt
        // args[4] = output

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rating By Gender");
        job.setJarByClass(RatingByGender.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GenderRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GenderRatingMapper.class);

        job.addCacheFile(new URI(args[2] + "#users.txt"));
        job.addCacheFile(new URI(args[3] + "#movies.txt"));

        job.setReducerClass(GenderReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}