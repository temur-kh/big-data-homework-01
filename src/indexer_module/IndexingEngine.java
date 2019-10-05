package indexer_module;

import common.CorpusParser;

public class IndexingEngine {
    public static void main(String[] args) throws Exception {
        // input corpus files directory
        String corpusPath = args[1];
        // directory for job outputs to save in
        String outputDir = args[2];

        String parsedCorpusPath = CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_TEXT);
        String parsedCorpusURLTitlePath = CorpusParser.run(corpusPath, outputDir, CorpusParser.PARSE_URL_TITLE);
        String docCountPath = DocumentCounter.run(parsedCorpusPath, outputDir);
        String wordEnumPath = WordEnumerator.run(parsedCorpusPath, outputDir);

    }
}
