package indexing_engine;

import common.MapStrConvert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class VectorGenReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> pairs = new ArrayList<>();
        // For each string pair in values
        for (Text val : values) {
            pairs.add(val.toString());
        }
        HashMap<Integer, Double> wordId2count = MapStrConvert.stringPairs2Map(pairs, MapStrConvert.parseInt,
                MapStrConvert.parseDouble, MapStrConvert.sumDouble);
        context.write(key, new Text(MapStrConvert.map2String(wordId2count)));
    }
}
