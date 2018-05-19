import java.io.Serializable;

public class Sample implements Serializable {
    double[] x;
    double y;

    public Sample(double[] x, Double y) {
        this.x = x;
        this.y = y;
    }
}
