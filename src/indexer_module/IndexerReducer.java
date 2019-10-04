package indexer_module;

import common.MapStringConverter;
import common.MapStringConverter.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class IndexerReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, Double> word2count = new HashMap<>();
        // For each string pair in values
        for (Text val : values) {
            // Get object pair from string pair
            Pair<String, Double> obj = MapStringConverter.string2Pair(val.toString(),
                    MapStringConverter.parseString, MapStringConverter.parseDouble);
            if (obj == null) continue;
            // Add count to map
            if (word2count.containsKey(obj.key)) {
                word2count.put(obj.key, word2count.get(obj.key) + obj.value);
            } else {
                word2count.put(obj.key, obj.value);
            }
        }
        context.write(key, new Text(MapStringConverter.map2String(word2count)));
    }
}
