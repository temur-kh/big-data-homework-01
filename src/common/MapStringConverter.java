package common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MapStringConverter {
    private static final String KVSeparator = "=";
    private static final String PairSeparator = ";";

    public static final FromString<Double> parseDouble = Double::parseDouble;
    public static final FromString<Integer> parseInt = Integer::parseInt;
    public static final FromString<String> parseString = string -> string;
    public static final FromString<IntWritable> parseIntWritable = string -> new IntWritable(Integer.parseInt(string));
    public static final FromString<DoubleWritable> parseDoubleWritable = string -> new DoubleWritable(Integer.parseInt(string));

    private MapStringConverter() {
    }

    public static <K, V> String makeStringPair(K key, V value) {
        return key.toString() + KVSeparator + value.toString();
    }

    private static String mergeStringPairs(List<String> pairs) {
        if (pairs.isEmpty()) {
            return "";
        }
        int size = pairs.size();
        if (size == 1) {
            return pairs.get(0);
        }
        StringBuilder out = new StringBuilder(pairs.get(0));
        for (int i = 1; i < size; ++i) {
            out.append(PairSeparator).append(pairs.get(i));
        }
        return out.toString();
    }

    public static <K, V> String map2String(HashMap<K, V> map) {
        List<String> pairs = new LinkedList<>();
        for (K key : map.keySet()) {
            pairs.add(makeStringPair(key, map.get(key)));
        }
        return mergeStringPairs(pairs);
    }

    public static class Pair<K, V> {
        K key;
        V value;

        Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public String toString() {
            return makeStringPair(key, value);
        }
    }

    public interface FromString<T> {
        T convert(String string);
    }

    private static <K, V> Pair<K, V> string2Pair(String string, FromString<K> k2str, FromString<V> v2str) {
        String[] s = string.split(KVSeparator);
        K key = k2str.convert(s[0]);
        V value = v2str.convert(s[1]);
        return new Pair<>(key, value);
    }

    public static <K, V> HashMap<K, V> string2Map(String string, FromString<K> k2str, FromString<V> v2str) {
        String[] pairs = string.split(PairSeparator);
        HashMap<K, V> map = new HashMap<>();
        for (String pair: pairs) {
            Pair<K, V> obj = string2Pair(pair, k2str, v2str);
            map.put(obj.key, obj.value);
        }
        return map;
    }

}
