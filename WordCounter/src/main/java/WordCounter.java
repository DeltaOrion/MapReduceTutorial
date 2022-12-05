import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class WordCounter {

    public static final String HDFS_ROOT_URL="hdfs://localhost:9000";
    private final Configuration conf;
    private final Map<String,Integer> wordCount;

    public WordCounter() {
        this.conf = new Configuration();
        this.wordCount = new TreeMap<>();
    }

    public static void main(String[] args) throws Exception {
        WordCounter counter = new WordCounter();
        counter.run(args[0],args[1]);
    }

    private void run(String inputFile, String outputFile) {
        //read file
        System.out.println("Starting Word Count");
        long start = System.currentTimeMillis();

        try(InputStream in = readFile(inputFile)) {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line = reader.readLine();
                while(line !=null) {
                    processLine(line);
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        writeOutput(outputFile);
        long end = System.currentTimeMillis();
        System.out.println("Finished Counting - Results Written");
        System.out.println("Total Time Elapsed = "+(end-start)+"ms");
    }

    private void writeOutput(String fileLocation) {
        try(OutputStream os = createFile(fileLocation)) {
            try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
                for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                    writer.write(entry.getKey() + "    " + entry.getValue() + System.lineSeparator());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private OutputStream createFile(String fileLocation) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.create(new Path(fileLocation),true);
    }

    private InputStream readFile(String fileLocation) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.open(new Path(fileLocation));
    }

    private void processLine(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
            String word = tokenizer.nextToken();
            if(wordCount.containsKey(word)) {
                wordCount.put(word,wordCount.get(word)+1);
            } else {
                wordCount.put(word,1);
            }
        }
    }
}
