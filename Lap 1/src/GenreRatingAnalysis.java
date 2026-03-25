import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenreRatingAnalysis {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text movieId = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split(",\\s*");
            if (p.length < 3) return;

            movieId.set(p[1].trim());
            outVal.set("R:" + p[2].trim());
            context.write(movieId, outVal);
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text movieId = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split(",\\s*", 3);
            if (p.length < 3) return;

            movieId.set(p[0].trim());
            outVal.set("G:" + p[2].trim());
            context.write(movieId, outVal);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> genres = new java.util.ArrayList<>();
            List<Double> ratings = new java.util.ArrayList<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("G:")) {
                    String g = s.substring(2);
                    genres.addAll(Arrays.asList(g.split("\\|")));
                } else if (s.startsWith("R:")) {
                    ratings.add(Double.parseDouble(s.substring(2)));
                }
            }

            for (String g : genres) {
                for (Double r : ratings) {
                    context.write(new Text(g), new Text(String.valueOf(r)));
                }
            }
        }
    }

    public static class GenreAvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text genre = new Text();
        private final DoubleWritable rating = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] p = value.toString().split("\t");
            if (p.length < 2) return;

            genre.set(p[0].trim());
            rating.set(Double.parseDouble(p[1].trim()));
            context.write(genre, rating);
        }
    }

    public static class GenreAvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
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
            context.write(key, new Text(String.format("%.2f (TotalRatings: %d)", avg, count)));
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = ratings_1.txt
        // args[1] = ratings_2.txt
        // args[2] = movies.txt
        // args[3] = temp_output
        // args[4] = final_output

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Genre Join");
        job1.setJarByClass(GenreRatingAnalysis.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, MovieMapper.class);

        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

        if (!job1.waitForCompletion(true)) System.exit(1);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Genre Average");
        job2.setJarByClass(GenreRatingAnalysis.class);

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        job2.setMapperClass(GenreAvgMapper.class);
        job2.setReducerClass(GenreAvgReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}