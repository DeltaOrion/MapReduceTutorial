import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataSource {

    private final List<Course> courses;
    private final List<String> firstNames;
    private final List<String> lastNames;
    private double averageAge = 17.5;
    private double ageRange = 0.75;
    private final Random random;

    private int averageGrade = 35;
    private int gradeRange = 25;

    private long studentNumber = 13872634;

    private final static double MAX_GRADE = 99.95;
    private final static double MIN_GRADE = 0;

    public DataSource() {
        this.courses = new ArrayList<>();
        this.lastNames = new ArrayList<>();
        this.firstNames = new ArrayList<>();
        this.random = new Random();
    }

    public Student randomStudent() {
        return new Student(randomName(),
                String.valueOf(randomStudentNumber()),
                random.nextDouble()*2,
                randomAge());
    }

    private int randomAge() {
        return (int) Math.round(random.nextGaussian() * ageRange + averageAge);
    }

    private String randomName() {
        return firstNames.get(random.nextInt(firstNames.size())) +
                " " +
                lastNames.get(random.nextInt(lastNames.size()));
    }

    public double randomGrade(Course course, Student student) {
        return clamp((random.nextGaussian() * gradeRange + course.getMean()) * student.getPerformanceFactor(),MIN_GRADE,MAX_GRADE);
    }

    private double clamp(double val, double min, double max) {
        if(val < min)
            return min;

        if(val > max)
            return max;

        return val;
    }

    private final static int ENGLISH_END = 6;

    public Course randomEnglish() {
        return courses.get(random.nextInt(ENGLISH_END));
    }

    public Course randomCourse() {
        return courses.get(random.nextInt(courses.size()-ENGLISH_END)+ENGLISH_END);
    }

    private boolean add = false;

    private long randomStudentNumber() {
        if(add) {
            return studentNumber+=2;
        } else {
            return studentNumber-=1;
        }
    }

    public int randomSubjectCount() {
        return random.nextInt(2)+4;
    }

    public void addCourse(Course course) {
        this.courses.add(course);
    }

    public void addCourses(List<Course> courses) {
        this.courses.addAll(courses);
    }

    public void addFirstName(String name) {
        this.firstNames.add(name);
    }

    public void addFirstNames(List<String> firstNames) {
        this.firstNames.addAll(firstNames);
    }

    public void addLastName(String name) {
        this.lastNames.add(name);
    }

    public void addLastNames(List<String> lastNames) {
        this.lastNames.addAll(lastNames);
    }

    public double getAverageAge() {
        return averageAge;
    }

    public void setAverageAge(double averageAge) {
        this.averageAge = averageAge;
    }

    public double getAgeRange() {
        return ageRange;
    }

    public void setAgeRange(double ageRange) {
        this.ageRange = ageRange;
    }

    public int getAverageGrade() {
        return averageGrade;
    }

    public void setAverageGrade(int averageGrade) {
        this.averageGrade = averageGrade;
    }

    public int getGradeRange() {
        return gradeRange;
    }

    public void setGradeRange(int gradeRange) {
        this.gradeRange = gradeRange;
    }
}
