package atar;

public class Course {

    private final String name;
    private double averageGrade;

    public Course(String name, double averageGrade) {
        this.name = name;
        this.averageGrade = averageGrade;
    }

    public Course(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public double getAverageGrade() {
        return averageGrade;
    }

    public double getScalingFactor() {
        return ((-1.0/125.0) * averageGrade) + 1.4;
    }

    public void setAverageGrade(double averageGrade) {
        this.averageGrade = averageGrade;
    }
}
