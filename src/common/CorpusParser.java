package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class CorpusParser {
    public static final int PARSE_TEXT = 0;
    public static final int PARSE_URL_TITLE = 1;

    public static final String UrlTitleSeparator = " ";

    public static final IntWritable zero = new IntWritable(0);

    public static class DocTextMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                JSONObject jb = new JSONObject(line);
                String sid = jb.getString("id");
                int id = Integer.parseInt(sid);
                String text = TextParser.parse(jb.getString("text"));
                context.write(new IntWritable(id), new Text(text));
            } catch (JSONException e) {
                context.write(zero, new Text("error"));
            }
        }
    }

    public static class DocUrlTitleMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                JSONObject jb = new JSONObject(line);
                String sid = jb.getString("id");
                int id = Integer.parseInt(sid);
                String url = jb.getString("url");
                String title = jb.getString("title");
                context.write(new IntWritable(id), new Text(url + UrlTitleSeparator + title));
            } catch (JSONException e) {
                context.write(zero, new Text("error"));
            }
        }
    }

    public static class DocReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> texts, Context context)
                throws IOException, InterruptedException {
            StringBuilder outputText = new StringBuilder();
            for (Text text : texts) {
                outputText.append(text.toString());
            }
            context.write(key, new Text(outputText.toString()));
        }
    }

    public static Path run(Path inputPath, Path outputDir, int parseMode) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Corpus_Parser");
        job.setJarByClass(CorpusParser.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fst = fs.listStatus(inputPath);

        for (FileStatus fsi : fst) {
            if (parseMode == PARSE_TEXT) {
                MultipleInputs.addInputPath(job, fsi.getPath(), TextInputFormat.class, DocTextMapper.class);
            } else if (parseMode == PARSE_URL_TITLE) {
                MultipleInputs.addInputPath(job, fsi.getPath(), TextInputFormat.class, DocUrlTitleMapper.class);
            } else {
                throw new Exception();
            }
        }

        Path outputPath = new Path(outputDir, String.format("corpus_parser_%d_mode", parseMode));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(DocReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true)) {
            return outputPath;
        } else {
            throw new Exception();
        }
    }
}
