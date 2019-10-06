package indexing_engine.modules;

import common.TextParser.DocIdText;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    private Set<String> setOfWords = new HashSet<>();
    private Text wordText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        DocIdText doc_text = new DocIdText(value);
        String[] words = doc_text.getWords();
        setOfWords.addAll(Arrays.asList(words));
        for (String word : setOfWords) {
            wordText.set(word);
            context.write(wordText, one);
        }
    }
}
