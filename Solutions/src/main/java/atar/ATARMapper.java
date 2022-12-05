package atar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import studentrank.KeyValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class ATARMapper extends Mapper<Object, Text,Text,DoubleWritable> {

    private Map<String,Course> courses;
    private TreeSet<KeyValue> sorter;

    //define the comma split regex (more efficient).
    private final static Pattern COMMA_SPLIT = Pattern.compile(",");
    private final static Pattern COLON_SPLIT = Pattern.compile(":");

    private final static int N = 100;

    @Override
    public void setup(Context context) {
        courses = new HashMap<>();
        this.sorter = new TreeSet<>();

        Path gradePath = new Path(FileOutputFormat.getOutputPath(context).getParent(),"grades");
        readCourses(gradePath,context.getConfiguration());
        if(courses.size()==0)
            throw new IllegalArgumentException("No working");
    }

    @Override
    public void map(Object o, Text input,
                    Context context) throws IOException, InterruptedException {
        String line = input.toString();
        //split the line by commas
        //ALTERNATIVE - String[] studentFields = line.split(",");
        String[] studentFields = COMMA_SPLIT.split(line);

        //setup array for the grades
        double[] grades = new double[5];

        //add the students english grade and the top
        //four courses other than english. After that calculate the actual ATAR.
        grades[0] = getGrade(studentFields[3]);
        getTopFourGrades(grades,studentFields);
        double ATAR = calculateATAR(grades);

        KeyValue v = new KeyValue(studentFields[1], ATAR);
        //insert the item into the treemap so it can be sorted
        sorter.add(v);
        //remove the smallest item in the TreeSet if there are
        //more than 100 items.
        if(sorter.size()>N)
            sorter.pollLast();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(KeyValue value : sorter) {
            context.write(new Text(value.getKey()),new DoubleWritable(value.getValue()));
        }
    }

    private double calculateATAR(double[] grades) {
        double sum = 0;
        int n = 0;
        for(double grade : grades) {
            n++;
            sum += grade;
        }

        return sum / n;
    }

    private void getTopFourGrades(double[] grades , String[] studentFields) {
        //get the top three grades other than english
        TreeSet<Double> topThreeGrades = new TreeSet<>((o1, o2) -> Double.compare(o2,o1));
        for(int i=4;i<studentFields.length;i++) {
            topThreeGrades.add(getGrade(studentFields[i]));
            if(topThreeGrades.size()>4)
                topThreeGrades.pollLast();
        }

        int count = 1;
        for(double grade : topThreeGrades) {
            grades[count] = grade;
            count++;
        }
    }


    private double getGrade(String courseField) {
        //create the key-value pair for the course
        //split the course by colon to separate the course from the grade.
        //alternative studentFields[i].split(":");
        String[] courseGrade = COLON_SPLIT.split(courseField);
        Course course = courses.get(courseGrade[0]);
        double grade = Double.parseDouble(courseGrade[1]);

        return clamp(0,course.getScalingFactor() * grade,99.5);
    }

    private double clamp(double min, double val, double max) {
        if(val < min)
            return min;

        return Math.min(val, max);

    }

    private void readCourses(Path gradeOut, Configuration configuration) {
        try(InputStream grades = getGradeFile(gradeOut,configuration)) {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(grades))) {
                String line = reader.readLine();
                while (line!=null) {
                    StringTokenizer tokenizer = new StringTokenizer(line);
                    String name = tokenizer.nextToken();
                    double avgGrade = Double.parseDouble(tokenizer.nextToken());
                    courses.put(name,new Course(name,avgGrade));
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private InputStream getGradeFile(Path gradeOut, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        return fs.open(new Path(gradeOut,"part-00000"));
    }
}
