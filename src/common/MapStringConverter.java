package common;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MapStringConverter {
    private static final String KVSeparator = "=";
    private static final String PairSeparator = ";";
    private static final String FileKVSeparator = "\t";

    public static final FromString<Double> parseDouble = Double::parseDouble;
    public static final FromString<Integer> parseInt = Integer::parseInt;
    public static final FromString<String> parseString = string -> string;

    public static final ValueCombinator selectFormer = (v1, v2) -> v1;
    public static final ValueCombinator selectLatter = (v1, v2) -> v2;
    public static final ValueCombinator<Integer> sumInt = Integer::sum;
    public static final ValueCombinator<Double> sumDouble = Double::sum;

    private MapStringConverter() {
    }

    public static <K, V> String makeStringPair(K key, V value) {
        return key.toString() + KVSeparator + value.toString();
    }

    public static <K, V> String map2String(HashMap<K, V> map) {
        List<String> pairs = new ArrayList<>();
        for (K key : map.keySet()) {
            pairs.add(makeStringPair(key, map.get(key)));
        }
        return String.join(PairSeparator, pairs);
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

    public interface ValueCombinator<T> {
        T combine(T v1, T v2);
    }

    public static <K, V> Pair<K, V> string2Pair(String string, FromString<K> k2str, FromString<V> v2str, String separator) {
        if (string.isEmpty()) return null;

        String[] s = string.split(separator);
        K key = k2str.convert(s[0]);
        V value = v2str.convert(s[1]);
        return new Pair<K, V>(key, value);
    }

    public static <K, V> HashMap<K, V> string2Map(Iterable<String> pairs, FromString<K> k2str, FromString<V> v2str,
                                                  ValueCombinator<V> comb, String separator) {
        HashMap<K, V> map = new HashMap<>();
        for (String pair : pairs) {
            Pair<K, V> obj = string2Pair(pair, k2str, v2str, separator);
            if (obj == null) continue;
            if (map.containsKey(obj.key)) {
                map.put(obj.key, comb.combine(map.get(obj.key), obj.value));
            } else {
                map.put(obj.key, obj.value);
            }
        }
        return map;
    }

    public static <K, V> HashMap<K, V> string2Map(Iterable<String> pairs, FromString<K> k2str, FromString<V> v2str,
                                            ValueCombinator<V> comb) {
        return string2Map(pairs, k2str, v2str, comb, KVSeparator);
    }

    public static <K, V> HashMap<K, V> string2Map(Iterable<String> pairs, FromString<K> k2str, FromString<V> v2str) {
        return string2Map(pairs, k2str, v2str, selectLatter, KVSeparator);
    }

    public static <K, V> HashMap<K, V> string2Map(String string, FromString<K> k2str, FromString<V> v2str,
                                                  ValueCombinator<V> comb, String separator) {
        return string2Map(Arrays.asList(string.split(PairSeparator)), k2str, v2str, comb, separator);
    }

    public static <K, V> HashMap<K, V> string2Map(String string, FromString<K> k2str, FromString<V> v2str,
                                                  ValueCombinator<V> comb) {
        return string2Map(string, k2str, v2str, comb, KVSeparator);
    }

    public static <K, V> HashMap<K, V> string2Map(String string, FromString<K> k2str, FromString<V> v2str) {
        return string2Map(string, k2str, v2str, selectLatter);
    }

    public static <K, V> HashMap<K, V> file2Map(String path, FromString<K> k2str, FromString<V> v2str,
                                          ValueCombinator<V> comb) throws IOException {
        FileReader r = new FileReader(new Path(path).getName());
        BufferedReader reader = new BufferedReader(r);
        ArrayList<String> pairs = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            pairs.add(line.trim());
        }
        reader.close();
        return string2Map(pairs, k2str, v2str, comb, FileKVSeparator);
    }

    public static <K, V> HashMap<K, V> file2Map(String path, FromString<K> k2str, FromString<V> v2str) throws IOException {
        return file2Map(path, k2str, v2str, selectLatter);
    }

    public static HashMap<String, Integer> fileStrInt2Map(String path) throws IOException {
        return file2Map(path, parseString, parseInt);
    }

    public static HashMap<Integer, String> fileIntStr2Map(String path) throws IOException {
        return file2Map(path, parseInt, parseString);
    }
}
