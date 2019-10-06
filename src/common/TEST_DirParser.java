package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TEST_DirParser {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) return;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        System.out.println(MapStrConvert.hdfsDirIntStr2Map(fs, path).toString());
    }
}
