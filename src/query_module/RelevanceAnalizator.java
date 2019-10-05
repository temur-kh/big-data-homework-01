package query_module;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common.MapStringConverter;

public class RelevanceAnalizator {

    public static class RelevanceMapper
            extends Mapper<IntWritable, Text, IntWritable, DoubleWritable> {

        private DoubleWritable resultDouble = new DoubleWritable();

        public void map(IntWritable docId, Text docVectorString, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String queryVectorString = conf.get("query_vector");
            Map<IntWritable, DoubleWritable> queryVector = MapStringConverter.string2Map(queryVectorString, MapStringConverter.parseIntWritable, MapStringConverter.parseDoubleWritable);
            Map<IntWritable, DoubleWritable> docVector = MapStringConverter.string2Map(docVectorString.toString(), MapStringConverter.parseIntWritable, MapStringConverter.parseDoubleWritable);

            Map<IntWritable, DoubleWritable> leftVector, rightVector;
            leftVector = queryVector.size() < docVector.size() ? queryVector : docVector;
            rightVector = queryVector.size() > docVector.size() ? queryVector : docVector;

            double result = 0;
            for (IntWritable wordId : leftVector.keySet()) {
                if (rightVector.containsKey(wordId)) {
                    double leftValue = leftVector.get(wordId).get();
                    double rightValue = rightVector.get(wordId).get();
                    result += leftValue * rightValue;
                }
            }
            resultDouble.set(result);
            context.write(docId, resultDouble);
        }
    }

    public static class SwapReducer extends Reducer<IntWritable, DoubleWritable, DoubleWritable, IntWritable> {
        public void reduce(IntWritable docId, Iterable<DoubleWritable> listOfResults, Context context)
                throws java.io.IOException, InterruptedException {

            for (DoubleWritable value : listOfResults) {
                context.write(value, docId);
            }
        }
    }

    public static class RankComparator extends WritableComparator {

        public RankComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            // compare for a reverse order
            return super.compare(key1, key2) * (-1);
        }
    }

    public void run(String[] args) throws Exception {
        String queryVectorString = args[2];
        Configuration conf = new Configuration();
        conf.set("query_vector", queryVectorString);

        Job job = Job.getInstance(conf, "DocumentCount");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setReducerClass(SwapReducer.class);
        job.setSortComparatorClass(RankComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}