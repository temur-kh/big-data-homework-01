package indexer_module;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Indexer {
    public static final String JobName = "indexer";
    public static final String StringIDF = "indexer.idf";
    public static final String StringWords = "indexer.words";

    public static String run(String path_docId2text, String path_word2Id, String path_word2idf, String path_outDir)
            throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMapper.class);
        job.setCombinerClass(IndexerReducer.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // Add words and idf to cache
        job.addCacheFile(new Path(path_word2Id).toUri());
        job.addCacheFile(new Path(path_word2idf).toUri());

        FileInputFormat.addInputPath(job, new Path(path_docId2text));
        Path out = new Path(path_outDir, "document_vectors");
        FileOutputFormat.setOutputPath(job, out);

        if (job.waitForCompletion(true)) {
            return out.toString();
        }
        throw new Exception("Indexer.run was not completed");
    }
}
