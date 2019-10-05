package indexer_module;

import common.MapStringConverter;
import common.WordCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class IndexerMapper
        extends Mapper<IntWritable, Text, IntWritable, Text> {

    private HashMap<String, Integer> word2Id;
    private HashMap<String, Integer> word2IDF;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        String path_words = conf.get(Indexer.StringWords);
        String path_idf = conf.get(Indexer.StringIDF);
        URI[] filesURIs = Job.getInstance(conf).getCacheFiles();
        for (URI uri : filesURIs) {
            String path = uri.getPath();
            if (path.equals(path_words)) {
                word2Id = MapStringConverter.fileStrInt2Map(path);
            } else if (path.equals(path_idf)) {
                word2IDF = MapStringConverter.fileStrInt2Map(path);
            }
        }
    }

    @Override
    public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> doc_map = WordCounter.countWords(value.toString());
        // Write results normalized by word's IDF
        for (String word : doc_map.keySet()) {
            Integer word_id = word2Id.get(word);
            double word_idf = word2IDF.get(word).doubleValue();
            Double norm_count = doc_map.get(word).doubleValue() / word_idf;
            // Convert to map pair
            String pair = MapStringConverter.makeStringPair(word_id, norm_count);
            value.set(pair);
            context.write(key, value);
        }
    }
}
