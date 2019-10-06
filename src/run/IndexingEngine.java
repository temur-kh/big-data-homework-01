package run;

import indexing_engine.CorpusParser;
import indexing_engine.DocumentCounter;
import indexing_engine.Indexer;
import indexing_engine.WordEnumerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IndexingEngine {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage:\n$hadoop jar <jar_name>.jar run.IndexingEngine " +
                    "<path to docs on HDFS> <path to output directory on HDFS>");
            return;
        }
        // input corpus files directory
        Path corpusPath = new Path(args[0]);
        // directory for job outputs to save in
        Path outputDir = new Path(args[1]);
        // Remove output dir, if exists
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        fs.close();
        // Run modules
        Path parsedCorpusPath = CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_TEXT);
        CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_TITLE_URL);
        Path docCountPath = DocumentCounter.run(parsedCorpusPath, outputDir);
        Path wordEnumPath = WordEnumerator.run(parsedCorpusPath, outputDir);
        Indexer.run(parsedCorpusPath, wordEnumPath, docCountPath, outputDir);
    }
}
