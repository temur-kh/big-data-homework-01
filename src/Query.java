import common.TextParser;
import ranker_engine.Ranker;

import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class Query {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage:\n$hadoop jar <jar_name>.jar Query " +
                    "<path to output directory of IndexingEngine on HDFS> " +
                    "<query in quotes> <number of most relevant docs>");
            return;
        }
        // Get arguments
        String indexer_output = args[0];
        String query = TextParser.parse(args[1]);
        int doc_number = Integer.parseInt(args[2]);
        // Run ranker
        ArrayList<String> docs = Ranker.run(indexer_output, query, doc_number);
        // Join docs
        String docs_inline = String.join("\n", docs);
        // Print output
        System.out.println(docs_inline);
        // Write output
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");
        FileWriter fw = new FileWriter("query_" + dateTime.format(formatter) + ".txt");
        fw.write(docs_inline);
        fw.close();
    }
}
