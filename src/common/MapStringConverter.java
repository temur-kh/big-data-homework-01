package common;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MapStringConverter {
    public static final String KVSeparator = "=";
    public static final String PairSeparator = ";";

    public static final FromString<Double> parseDouble = Double::parseDouble;
    public static final FromString<Integer> parseInt = Integer::parseInt;
    public static final FromString<String> parseString = string -> string;

    private MapStringConverter() {
    }

    public static <K, V> String makeStringPair(K key, V value) {
        return key.toString() + KVSeparator + value.toString();
    }

    public static String mergeStringPairs(List<String> pairs) {
        if (pairs.isEmpty()) {
            return "";
        }
        int size = pairs.size();
        if (size == 1) {
            return pairs.get(0);
        }
        String out = pairs.get(0);
        for (int i = 1; i < size; ++i) {
            out += PairSeparator + pairs.get(i);
        }
        return out;
    }

    public static <K, V> String map2String(HashMap<K, V> map) {
        List<String> pairs = new ArrayList<>();
        Set<K> keys = map.keySet();
        for (K key : keys) {
            pairs.add(makeStringPair(key, map.get(key)));
        }
        return mergeStringPairs(pairs);
    }

    public static class Pair<K, V> {
        public K key;
        public V value;

        public Pair(K key, V value) {
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

    public static <K, V> Pair<K, V> string2Pair(String string, FromString<K> k2str, FromString<V> v2str) {
        if (string.isEmpty()) return null;

        String[] s = string.split(KVSeparator);
        K key = k2str.convert(s[0]);
        V value = v2str.convert(s[1]);
        return new Pair<K, V>(key, value);
    }

    public static <K, V> HashMap<K, V> string2Map(String string, FromString<K> k2str, FromString<V> v2str) {
        HashMap<K, V> map = new HashMap<>();
        if (string.isEmpty()) return map;

        String[] pairs = string.split(PairSeparator);
        for (String pair : pairs) {
            Pair<K, V> obj = string2Pair(pair, k2str, v2str);
            if (obj != null) map.put(obj.key, obj.value);
        }
        return map;
    }

}
