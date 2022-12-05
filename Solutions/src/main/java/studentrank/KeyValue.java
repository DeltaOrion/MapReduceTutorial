package studentrank;

/**
 * Define a custom class. This simply contains a the key and the value for the output. The class
 * implements comparable so that the TreeSet knows how to order the values!
 */
public class KeyValue implements Comparable<KeyValue> {
    private final String key;
    private final double value;

    public KeyValue(String key, double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(KeyValue o) {
        //compare in descending order
        //You may need to define your own comparator
        return Double.compare(o.value,this.value);
    }

    public String getKey() {
        return key;
    }

    public double getValue() {
        return value;
    }
}