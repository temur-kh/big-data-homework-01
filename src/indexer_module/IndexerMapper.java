package indexer_module;

import common.MapStringConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

public class IndexerMapper
        extends Mapper<IntWritable, Text, IntWritable, Text> {
    public static final String Separator = "\t";

    private HashMap<String, Integer> word2id = new HashMap<>();
    private HashMap<String, Integer> word2idf = new HashMap<>();

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        URI[] filesURIs = Job.getInstance(conf).getCacheFiles();
        // Parse words
        parseWords(new Path(filesURIs[0].getPath()).getName());
        // Parse idf
        parseIDF(new Path(filesURIs[1].getPath()).getName());
    }

    private void parseWords(String filename) throws IOException {
        parseStringInt(filename, word2id);
    }

    private void parseIDF(String filename) throws IOException {
        parseStringInt(filename, word2idf);
    }

    private void parseStringInt(String filename, HashMap<String, Integer> map) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] word_id = line.trim().split(Separator);
            map.put(word_id[0], Integer.parseInt(word_id[1]));
        }
        reader.close();
    }

    @Override
    public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokens = new StringTokenizer(value.toString());
        HashMap<String, Integer> doc_map = new HashMap<>();
        // Count words for this doc
        while (tokens.hasMoreTokens()) {
            String word = tokens.nextToken();
            if (doc_map.containsKey(word)) {
                doc_map.put(word, doc_map.get(word) + 1);
            } else {
                doc_map.put(word, 1);
            }
        }
        // Write results normalized by word's IDF
        Set<String> keys = doc_map.keySet();
        for (String word : keys) {
            Integer word_id = word2id.get(word);
            double word_idf = word2idf.get(word).doubleValue();
            Double norm_count = doc_map.get(word).doubleValue() / word_idf;
            // Convert to map pair
            String pair = MapStringConverter.makeStringPair(word_id, norm_count);
            value.set(pair);
            context.write(key, value);
        }
    }
}
