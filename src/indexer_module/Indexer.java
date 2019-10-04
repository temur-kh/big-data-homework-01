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
import org.apache.hadoop.util.GenericOptionsParser;

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Indexer {
    public static final String JobName = "indexer";
    public static final String FileIDF = "indexer.idf";
    public static final String FileWords = "indexer.words";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // args[0] = main class
        // args[1] = path to json file with docs
        // args[2] = path to (word - id) file
        // args[3] = path to (word - number of occurrences in docs) file
        // args[4] = path to output folder
        String path_json = args[1];
        String path_words = args[2];
        String path_idf = args[3];
        String path_out = args[4];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMapper.class);
        job.setCombinerClass(IndexerReducer.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // Add words and idf to cache
        job.addCacheFile(new Path(path_words).toUri());
        job.addCacheFile(new Path(path_idf).toUri());

        FileInputFormat.addInputPath(job, new Path(path_json));
        FileOutputFormat.setOutputPath(job, new Path(path_out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
