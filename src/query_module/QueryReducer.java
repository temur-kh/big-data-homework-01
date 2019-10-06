package query_module;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class QueryReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, Text> {
    @Override
    public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> docs = new ArrayList<>();
        for (IntWritable val : values) {
            docs.add(val.toString());
        }
        context.write(key, new Text(String.join(Query.OutputDocSeparator, docs)));
    }
}
