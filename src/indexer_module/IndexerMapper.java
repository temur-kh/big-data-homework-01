package indexer_module;

import common.MapStrConvert;
import common.TextParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class IndexerMapper extends Mapper<Object, Text, IntWritable, Text> {
    private HashMap<String, Integer> word2Id;
    private HashMap<String, Integer> word2IDF;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path path_words = new Path(conf.get(Indexer.StringWords));
        Path path_idf = new Path(conf.get(Indexer.StringIDF));
        word2Id = MapStrConvert.hdfsDirStrInt2Map(fs, path_words);
        word2IDF = MapStrConvert.hdfsDirStrInt2Map(fs, path_idf);
//        fs.close();  // - will produce an error
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        TextParser parser = new TextParser(value);
        IntWritable doc_id = new IntWritable(parser.docId);
        HashMap<String, Integer> doc_map = parser.countWords();
        // Write results normalized by word's IDF
        for (String word : doc_map.keySet()) {
            Integer word_id = word2Id.get(word);
            double word_idf = word2IDF.get(word).doubleValue();
            Double norm_count = doc_map.get(word).doubleValue() / word_idf;
            // Convert to map pair
            String pair = MapStrConvert.makeStringPair(word_id, norm_count);
            value.set(pair);
            context.write(doc_id, value);
        }
    }
}
