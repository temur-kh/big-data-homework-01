package indexer_module;

import common.MapStringConverter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class IndexerReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> pairs = new ArrayList<>();
        // For each string pair in values
        for (Text val : values) {
            pairs.add(val.toString());
        }
        HashMap<Integer, Double> wordId2count = MapStringConverter.stringPairs2Map(pairs, MapStringConverter.parseInt,
                MapStringConverter.parseDouble, MapStringConverter.sumDouble);
        context.write(key, new Text(MapStringConverter.map2String(wordId2count)));
    }
}
