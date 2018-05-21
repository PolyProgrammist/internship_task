import java.io.Serializable;

public class Sample implements Serializable {
    double[] row;
    double val;

    public Sample(double[] row, Double val) {
        this.row = row;
        this.val = val;
    }
}
