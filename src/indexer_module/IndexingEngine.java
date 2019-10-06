package indexer_module;

import org.apache.hadoop.fs.Path;

public class IndexingEngine {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage:\n$hadoop jar <jar_name>.jar IndexingEngine " +
                    "<path to docs on HDFS> <path to output directory on HDFS>");
            return;
        }
        // input corpus files directory
        Path corpusPath = new Path(args[1]);
        // directory for job outputs to save in
        Path outputDir = new Path(args[2]);

        Path parsedCorpusPath = CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_TEXT);
        Path parsedCorpusURLTitlePath = CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_TITLE_URL);
        Path docCountPath = DocumentCounter.run(parsedCorpusPath, outputDir);
        Path wordEnumPath = WordEnumerator.run(parsedCorpusPath, outputDir);
        Path docVectorsPath = Indexer.run(parsedCorpusPath, wordEnumPath, docCountPath, outputDir);
    }
}
