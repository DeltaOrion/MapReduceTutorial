import java.util.Random;

public class Course {

    private final String name;
    private final double mean;

    public Course(String name, double mean) {
        this.name = name;
        this.mean = mean;
    }

    public String getName() {
        return name;
    }

    public double getMean() {
        return mean;
    }
}
