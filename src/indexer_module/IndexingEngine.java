package indexer_module;

import common.CorpusParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexingEngine {
    public static void main(String[] args) throws Exception {
        // input corpus files directory
        String corpusPath = args[1];
        // directory for job outputs to save in
        String outputDir = args[2];
//        String parsedCorpusPath = CorpusParser.main(args);
        String docCountPath = DocumentCounter.run(corpusPath, outputDir);
        String wordEnumPath = WordEnumerator.run(corpusPath, outputDir);
    }
}
