import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

public class DataGenerator {

    /**
     * 1GB = 500000 Entries
     */

    private final static DataGenerator generator = new DataGenerator();
    private final static String COURSE_LOCATION = "course.txt";
    private final static String FNAME_LOCATION = "first_name.txt";
    private final static String LNAME_LOCATION = "last_name.txt";
    public static final String HDFS_ROOT_URL="hdfs://localhost:9000";

    private final Configuration conf;
    private final DataSource source;

    public static void main(String[] args) {
        generator.run(args[0],toBytes(args[1]));
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

    public DataGenerator() {
        this.source = new DataSource();
        this.conf = new Configuration();
    }

    public void run(String location ,long bytes) {
        System.out.println("Reading Sources");
        loadDataSources();
        long start = System.currentTimeMillis();
        System.out.println("Generating Entries");
        generate(location, bytes);
        long end = System.currentTimeMillis();
        System.out.println("Total Elapsed Time - "+(end - start)+"ms");
    }

    private void generate(String location ,long bytes) {
        long sizeCount = 0;
        try (OutputStream os = createFile(location, bytes)) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
                while (sizeCount < bytes) {
                    StringBuilder result = new StringBuilder();
                    Student student = source.randomStudent();
                    Course english  = source.randomEnglish();

                    result.append(student.getStudentNumber())
                            .append(",")
                            .append(student.getName())
                            .append(",")
                            .append(student.getAge())
                            .append(",")
                            .append(english.getName())
                            .append(":")
                            .append(formatGrade(source.randomGrade(english,student)))
                            .append(",");

                    int subjects = source.randomSubjectCount();
                    for(int i=0;i<subjects;i++) {
                        Course course = source.randomCourse();
                        result.append(course.getName())
                                .append(":")
                                .append(formatGrade(source.randomGrade(course,student)));

                        if(i<subjects-1) {
                            result.append(",");
                        }
                    }

                    sizeCount += result.length()+1;
                    writer.write(result.toString() + System.lineSeparator());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String formatGrade(double grade) {
        return String.format("%.2f",grade);
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

    private String getFileName(long size) {
        String name = "Students" + getDisplaySize(size);
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

    private void loadDataSources() {
        Random random = new Random();
        loadSource(COURSE_LOCATION, strings -> {
            for (String course : strings) {
                source.addCourse(new Course(course, random.nextInt(15) + 30));
            }
        });
        loadSource(LNAME_LOCATION, source::addFirstNames);
        loadSource(FNAME_LOCATION, source::addLastNames);
    }

    private void loadSource(String resourceLocation, Consumer<List<String>> consumer) {
        List<String> load = new ArrayList<>();
        try (InputStream courseFile = getClass().getResourceAsStream(resourceLocation)) {
            readDictionary(load, courseFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        consumer.accept(load);
    }

    private void readDictionary(List<String> load, InputStream file) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file))) {
            String line = reader.readLine();
            while (line != null) {
                load.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
