package query_module;

import common.MapStringConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class RelevanceAnalizator {

    public static class RelevanceMapper
            extends Mapper<IntWritable, Text, IntWritable, DoubleWritable> {

        private DoubleWritable resultDouble = new DoubleWritable();

        public void map(IntWritable docId, Text docVectorString, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String queryVectorString = conf.get("query_vector");
            Map<Integer, Double> queryVector = MapStringConverter.string2Map(queryVectorString, MapStringConverter.parseInt, MapStringConverter.parseDouble);
            Map<Integer, Double> docVector = MapStringConverter.string2Map(docVectorString.toString(), MapStringConverter.parseInt, MapStringConverter.parseDouble);

            Map<Integer, Double> leftVector, rightVector;
            leftVector = queryVector.size() < docVector.size() ? queryVector : docVector;
            rightVector = queryVector.size() > docVector.size() ? queryVector : docVector;

            double result = 0;
            for (Integer wordId : leftVector.keySet()) {
                if (rightVector.containsKey(wordId)) {
                    double leftValue = leftVector.get(wordId);
                    double rightValue = rightVector.get(wordId);
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

    public static String run(String queryVectorString, String docTFIDFPath, String outputDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query_vector", queryVectorString);

        Job job = Job.getInstance(conf, "DocumentCount");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setReducerClass(SwapReducer.class);
        job.setSortComparatorClass(RankComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(docTFIDFPath));
        Path outputPath = new Path(outputDir, "relevance_analyzer");
        FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true)) {
            return outputPath.toString();
        } else {
            throw new Exception();
        }
    }
}
