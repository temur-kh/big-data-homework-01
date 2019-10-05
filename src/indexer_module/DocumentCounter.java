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

public class DocumentCounter {

    public static class CounterMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private Set<String> setOfWords = new HashSet<>();

        private final static IntWritable one = new IntWritable(1);
        private Text wordText = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String nextWord = itr.nextToken();
                setOfWords.add(nextWord);
            }
            for (String word : setOfWords) {
                wordText.set(word);
                context.write(wordText, one);
            }
        }
    }

    public static class IDFReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text word, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int countOccurrences = 0;
            for (IntWritable val : values) {
                countOccurrences += val.get();
            }
            result.set(countOccurrences);
            context.write(word, result);
        }
    }

    public static Path run(Path inputPath, Path outputDir) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DocumentCount");
        job.setJarByClass(DocumentCounter.class);
        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(IDFReducer.class);
        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(outputDir, "document_counter");
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)) {
            return outputPath;
        } else {
            throw new Exception();
        }
    }
}
