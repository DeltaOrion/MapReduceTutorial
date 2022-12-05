public class Student {

    private final String name;
    private final String studentNumber;
    private final double performanceFactor;
    private final int age;

    public Student(String name, String studentNumber, double performanceFactor, int age) {
        this.name = name;
        this.studentNumber = studentNumber;
        this.performanceFactor = performanceFactor;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public String getStudentNumber() {
        return studentNumber;
    }

    public double getPerformanceFactor() {
        return performanceFactor;
    }

    public int getAge() {
        return age;
    }
}
