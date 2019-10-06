package common;

import common.MapStrConvert.Pair;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

public class TextParser {
    private static final String WordSeparator = " ";

    private TextParser() {
    }

    public static String parse(String string) {
        string = string.trim().replaceAll("[^a-zA-Z0-9-_'\\s]+", "");
        return string.replaceAll("\\s+", WordSeparator);
    }

    private static Pair<Integer, String> getDocIdText(Text value) {
        return MapStrConvert.string2Pair(value.toString(),
                MapStrConvert.parseInt, MapStrConvert.parseString, MapStrConvert.FileKVSeparator);
    }

    private static String[] getWords(String string) {
        return string.split(WordSeparator);
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

    public static class DocIdText {
        public Integer docId;
        public String text;

        public DocIdText(Text value) {
            Pair<Integer, String> p = getDocIdText(value);
            docId = p.key;
            text = p.value;
        }

        public String[] getWords() {
            return TextParser.getWords(text);
        }

        public HashMap<String, Integer> countWords() {
            return TextParser.countWords(text);
        }
    }

    public static class DocIdVector {
        public Integer docId;
        public HashMap<Integer, Double> vector;

        public DocIdVector(Text value) {
            Pair<Integer, String> p = getDocIdText(value);
            docId = p.key;
            vector = MapStrConvert.string2Map(p.value, MapStrConvert.parseInt, MapStrConvert.parseDouble);
        }
    }
}
