package indexer_module;

import common.TextParser;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class WordEnumerator {
    public static final String JobName = "word_enumeration";
    public static final String OutputDir = "word_enumerator";

    public static class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        private Set<String> setOfWords = new HashSet<>();
        private Text wordText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = TextParser.getWords(value);
            setOfWords.addAll(Arrays.asList(words));
            for (String word : setOfWords) {
                wordText.set(word);
                context.write(wordText, one);
            }
        }
    }

    public static class EnumerationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static int id = 0;

        public void reduce(Text word, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(word, new IntWritable(++id));
        }
    }

    public static Path run(Path inputPath, Path outputDir) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(WordEnumerator.class);
        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(EnumerationReducer.class);
        job.setReducerClass(EnumerationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(outputDir, OutputDir);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)) {
            return outputPath;
        } else {
            throw new Exception();
        }
    }
}