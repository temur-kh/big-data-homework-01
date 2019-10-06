package indexing_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DocumentCounter {
    private static final String JobName = "document_count";
    private static final String OutputDir = "document_counter";

    public static class IDFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text word, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(DocumentCounter.class);
        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(IDFReducer.class);
        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(outputDir, OutputDir);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)) {
            return outputPath;
        } else {
            throw new Exception("DocumentCounter.run() was not completed");
        }
    }
}
