package indexer_module;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordEnumerator {

    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private Set<String> words = new HashSet<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            token.nextToken();
            while (token.hasMoreTokens()) {
                String nextWord = token.nextToken();
                words.add(nextWord);
            }
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class EnumerationReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static int id = 0;

        public void reduce(Text word, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            this.id += 1;
            context.write(word, new IntWritable(this.id));
        }
    }

    public static String run(String inputPath, String outputDir) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordEnumeration");
        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(EnumerationReducer.class);
        job.setReducerClass(EnumerationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path outputPath = new Path(outputDir, "word_enumerator");
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)) {
            return outputPath.toString();
        } else {
            throw new Exception();
        }
    }
}