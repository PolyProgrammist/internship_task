import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class GradientDescent {
    private JavaRDD<Sample> df;
    private double[] coeffs;

    public GradientDescent(JavaRDD<Sample> df) {
        this.df = df;
    }

    private double[] newCoeffs(double[] coeffs) {
        double[] res = new double[coeffs.length];
        final double alpha = 0.0000003;
        for (int i = 0; i < coeffs.length; i++) {
            res[i] = coeffs[i] + alpha / df.count() * sum(i);
        }
        return res;
    }

    private double sum(int j) {
        double res = 0.0;
        for (Sample s: df.collect()) {
            res += (s.y - h(s.x)) * s.x[j];
        }
        return res;
    }

    private double h(double[] x, double[] coeffs) {
        double res = 0.0;
        for (int i = 0; i < x.length; i++) {
            res += x[i] * coeffs[i];
        }
        return res;
    }

    private double h(double[] x) {
        return h(x, coeffs);
    }

    private double rmse() {
        double res = 0.0;
        for (Sample s: df.collect()) {
            double t = s.y - h(s.x);
            res += t * t;
        }
        return res;
    }

    public void GetCoeffs() {
        long start = System.currentTimeMillis();
        coeffs = new double[df.first().x.length];
        for (int i = 0; i < coeffs.length; i++) {
            coeffs[i] = 0.0;
        }

        final int n = 10;
        for (int i = 0; i < n; i++) {
            System.out.println(rmse());
            for (double t: coeffs){
                System.out.print(t + " ");
            }
            System.out.println();
            double[] newcoeffs = newCoeffs(coeffs);
            coeffs = newcoeffs;
        }
        long stop = System.currentTimeMillis();
        System.out.println("Time is: " + String.valueOf(stop - start));
    }
}
