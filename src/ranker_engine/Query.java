package ranker_engine;

import common.MapStrConvert;
import common.TextParser;
import indexing_engine.modules.CorpusParser;
import indexing_engine.modules.Indexer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ranker_engine.modules.QueryMapper;
import ranker_engine.modules.QueryReducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Query {
    private static final String JobName = "query";
    public static final String StringIEPath = "query.ei_path";
    public static final String StringInput = "query.input";
    private static final String OutputDir = "query";
    public static final String OutputDocSeparator = "\\|";

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage:\n$hadoop jar <jar_name>.jar Query " +
                    "<path to output directory of IndexingEngine on HDFS> " +
                    "<query in quotes> <number of most relevant docs>");
            return;
        }
        // Get arguments
        String indexer_output = args[1];
        String query = TextParser.parse(args[2]);
        int doc_number = Integer.parseInt(args[3]);
        // Setup configuration
        Configuration conf = new Configuration();
        // Add words and idf to conf
        conf.set(StringIEPath, indexer_output);
        conf.set(StringInput, query);
        // Make job
        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(Query.class);
        job.setMapperClass(QueryMapper.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        Path output = new Path(indexer_output, OutputDir);
        FileInputFormat.addInputPath(job, new Path(indexer_output, Indexer.OutputDir));
        FileOutputFormat.setOutputPath(job, output);
        if (!job.waitForCompletion(true)) {
            return;
        }
        // Success, do output
        conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        // Read docId -> title URL
        Path path_titles = new Path(indexer_output, CorpusParser.OutputDir_TITLE_URL);
        HashMap<Integer, String> docId2TitleUrl = MapStrConvert.hdfsDirIntStr2Map(fs, path_titles);
        // Read output
        ArrayList<Output> outs = readMapRedOutput(fs, output);
        // Extract most relevant doc ids and add their title and url
        ArrayList<String> docs = new ArrayList<>();
        for (Output o : outs) {
            for (String docId : o.docs) {
                int id = Integer.parseInt(docId);
                docs.add(docId2TitleUrl.get(id));
                doc_number--;
                if (doc_number == 0) break;
            }
            if (doc_number == 0) break;
        }
        // Print output
        System.out.println(String.join("\n", docs));
    }

    public static class Output {
        double relevance;
        String[] docs;

        Output(String line) {
            String[] rel_docs = line.split(MapStrConvert.FileKVSeparator);
            docs = rel_docs[1].split(OutputDocSeparator);
            relevance = Double.parseDouble(rel_docs[0]);
        }
    }

    private static final Comparator<Output> compare = Comparator.comparingDouble(v -> v.relevance);

    private static ArrayList<Output> readMapRedOutput(FileSystem fs, Path path) throws IOException {
        ArrayList<Output> outs = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
        while (it.hasNext()) {
            FSDataInputStream inputStream = fs.open(it.next().getPath());
            String in = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            inputStream.close();
            if (in.isEmpty()) continue;
            String[] lines = in.split(MapStrConvert.FilePairSeparator);
            for (String line : lines) {
                outs.add(new Output(line));
            }
        }
        outs.sort(compare);
        return outs;
    }
}
