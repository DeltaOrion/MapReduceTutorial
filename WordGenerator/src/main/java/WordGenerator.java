import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WordGenerator {

    private final List<String> words;
    private final ExecutorService service = Executors.newCachedThreadPool();
    private CountDownLatch latch;

    private final Set<String> usedNames;

    private final static String DICTIONARY_LOCATION = "words_alpha.txt";
    public static final String HDFS_ROOT_URL="hdfs://localhost:9000";

    private final Configuration conf;

    public WordGenerator() {
        this.words = new ArrayList<>();
        this.usedNames = new HashSet<>();
        this.conf = new Configuration();
    }

    public static void main(String[] args) {
        WordGenerator generator = new WordGenerator();
        if(args.length==0) {
            System.err.println("Unknown File Path");
            return;
        }

        if(args.length==1) {
            System.err.println("No File Sizes specified.");
            return;
        }

        long[] nums = new long[args.length-1];
        for(int i=1;i<args.length;i++) {
            nums[i-1] = toBytes(args[i]);
        }

        generator.run(args[0],nums);
    }

    private static long toBytes(String bytes) {
        char suffix = bytes.charAt(bytes.length() - 1);
        long multiply = 1;
        boolean splice = true;
        if (suffix == 'T') {
            multiply = (long) (Math.pow(2, 40));
        } else if (suffix == 'G') {
            multiply = (long) (Math.pow(2, 30));
        } else if(suffix=='M') {
            multiply = (long) (Math.pow(2,20));
        } else if(suffix=='K') {
            multiply = (long) (Math.pow(2,10));
        } else {
            splice = false;
        }

        String num = bytes;
        if(splice)
            num = bytes.substring(0,bytes.length()-1);

        return multiply * Long.parseLong(num);
    }

    private void run(String outputLoc, long[] fileSizes) {
        System.out.println("Reading Dictionary");
        readWords();
        System.out.println("Finished Reading Dictionary");
        latch = new CountDownLatch(fileSizes.length);

        System.out.println("Beginning Write Operation");
        long start = System.currentTimeMillis();
        //write all the files concurrently
        for (long size : fileSizes) {
            service.submit(() -> writeFile(outputLoc, size));
        }

        try {
            latch.await();
            long end = System.currentTimeMillis();
            System.out.println("Total Elapsed Time - "+(end - start)+"ms");
        } catch (InterruptedException e) {
            System.err.println("The program was interrupted while writing words");
            Thread.currentThread().interrupt();
        }

        service.shutdown();
    }

    private void writeFile(String location, long size) {
        //100 words per line
        Random random = new Random();
        long sizeCount = 0;
        int wordsPlaced = 0;
        try (OutputStream os = createFile(location, size)) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
                while (sizeCount < size) {
                    String word = randomWord(random);
                    writer.write(word + " ");
                    wordsPlaced++;
                    if (wordsPlaced == 100) {
                        wordsPlaced = 0;
                        writer.write(System.lineSeparator());
                        sizeCount++;
                    }
                    sizeCount += (word.length() + 1); //length of the word plus the space
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        latch.countDown();
        System.out.println("Finished Writing "+size+" bytes");
    }

    private OutputStream createFile(String location, long size) throws IOException {
        String fileName = getFileName(size);
        /*
        File file = new File(location, fileName);
        file.createNewFile();
        return new FileOutputStream(file);
        */
        Path path = new Path(new Path(location),fileName);
        FileSystem fs = FileSystem.get(conf);
        return fs.create(path,true);
    }

    private synchronized String getFileName(long size) {
        String baseName = "Words" + getDisplaySize(size);
        String name = baseName;
        int counter = 1;
        while (usedNames.contains(name)) {
            name = baseName + "-" + counter;
            counter++;
        }

        usedNames.add(name);
        return name + ".txt";
    }

    private String getDisplaySize(long size) {
        int divide = (int) (size / Math.pow(2,40));
        if(divide>0)
            return divide + "TB";

        divide = (int) (size / Math.pow(2,30));
        if(divide>0)
            return divide + "GB";

        divide = (int) (size / Math.pow(2,20));
        if(divide>0)
            return divide + "MB";

        divide = (int) (size / Math.pow(2,10));
        if(divide>0)
            return divide + "KB";

        return String.valueOf(size);
    }

    private void readWords() {
        try (InputStream dictionaryFile = getClass().getResourceAsStream(DICTIONARY_LOCATION)) {
            if (dictionaryFile == null)
                throw new IOException("Cannot find dictionary " + DICTIONARY_LOCATION);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(dictionaryFile))) {
                String line = reader.readLine();
                while (line != null) {
                    words.add(line);
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String randomWord(Random random) {
        return words.get(random.nextInt(words.size()));
    }
}
