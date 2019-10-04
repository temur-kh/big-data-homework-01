package indexer_module;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexerReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {
}
