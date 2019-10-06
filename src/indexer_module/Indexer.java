package indexer_module;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Indexer {
    public static final String JobName = "indexer";
    public static final String StringIDF = "indexer.idf";
    public static final String StringWords = "indexer.words";

    public static Path run(Path path_docId2text, Path path_word2Id, Path path_word2IDF, Path path_outDir)
            throws Exception {
        Configuration conf = new Configuration();
        // Add words and idf to conf
        conf.set(StringWords, path_word2Id.toString());
        conf.set(StringIDF, path_word2IDF.toString());

        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMapper.class);
        job.setCombinerClass(IndexerReducer.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, path_docId2text);
        Path out = new Path(path_outDir, "document_vectors");
        FileOutputFormat.setOutputPath(job, out);

        if (job.waitForCompletion(true)) {
            return out;
        } else {
            throw new Exception("Indexer.run was not completed");
        }
    }
}
