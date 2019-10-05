package common;

import java.util.HashMap;
import java.util.StringTokenizer;

public class WordCounter {
    private WordCounter() {};

    public static HashMap<String, Integer> countWords(String string) {
        StringTokenizer tokens = new StringTokenizer(string);
        HashMap<String, Integer> map = new HashMap<>();
        while (tokens.hasMoreTokens()) {
            String word = tokens.nextToken();
            if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 1);
            }
        }
        return map;
    }
}
