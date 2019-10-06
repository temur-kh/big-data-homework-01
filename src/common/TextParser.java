package common;

import common.MapStrConvert.Pair;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

public class TextParser {
    public static final String WordSeparator = " ";

    public static String parse(String string) {
        string = string.trim().replaceAll("[^a-zA-Z0-9-_' ]+", "");
        return string.replaceAll("\\s+", WordSeparator);
    }

    private static Pair<Integer, String> getDocIdText(Text value) {
        return MapStrConvert.string2Pair(value.toString(),
                MapStrConvert.parseInt, MapStrConvert.parseString, MapStrConvert.FileKVSeparator);
    }

    public static String[] getWords(String string) {
        return string.split(WordSeparator);
    }

    public static String[] getWords(Text value) {
        return getDocIdText(value).value.split(WordSeparator);
    }

    public static HashMap<String, Integer> countWords(String string) {
        String[] words = getWords(string);
        HashMap<String, Integer> map = new HashMap<>();
        for (String word : words) {
            if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 1);
            }
        }
        return map;
    }

    public Integer docId;
    public String text;

    public TextParser(Text value) {
        Pair<Integer, String> p = getDocIdText(value);
        docId = p.key;
        text = p.value;
    }

    public String[] getWords() {
        return getWords(text);
    }

    public HashMap<String, Integer> countWords() {
        return countWords(text);
    }
}
