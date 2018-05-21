import org.apache.spark.api.java.JavaRDD;

public class GradientDescent {
    private JavaRDD<Sample> df;
    private Double precision = null;
    private Integer maxSteps = null;

    private double[] coeffs;
    private double alpha = 1;

    /**
     * Gradient Descent with default parameters.
     * <li>
     *     <ul>precision=1e-4</ul>
     *     <ul>maxSteps=1000</ul>
     * </li>
     * @param df The source of the data to find a local minimum
     */
    public GradientDescent(JavaRDD<Sample> df) {
        this.df = df;
        maxSteps = 1000;
        precision = 1e-4;
    }

    public GradientDescent(JavaRDD<Sample> df, Double precision) {
        this.df = df;
        this.precision = precision;
    }

    public GradientDescent(JavaRDD<Sample> df, Integer maxSteps) {
        this.df = df;
        this.maxSteps = maxSteps;
    }

    /**
     * The main constructor.
     * @param df The source of the data to find a local minimum
     * @param precision The a such that the method will stop
     *                  when the difference of rmse will be less than a
     * @param maxSteps The maximum number of steps for method
     */
    public GradientDescent(JavaRDD<Sample> df, Double precision, Integer maxSteps) {
        this.df = df;
        this.precision = precision;
        this.maxSteps = maxSteps;
    }

    /**
     * Gradient descent implementation.
     * @return double[] This method returns coefficients of the model.
     */
    public double[] getCoeffs() {
        final long start = System.currentTimeMillis();
        coeffs = new double[df.first().row.length];
        for (int i = 0; i < coeffs.length; i++) {
            coeffs[i] = 0.0;
        }
        int step = 0;
        while (true) {
            step++;
            System.out.println("rmse: " + rmse(coeffs));
            for (double t: coeffs) {
                System.out.print(t + " ");
            }
            System.out.println();
            double[] newcoeffs = newCoeffs(coeffs);
            double dif = rmse(coeffs) - rmse(newcoeffs);
            System.out.println("dif: " + dif);
            System.out.println("step: " + step);
            if (precision != null && dif < precision) {
                System.out.println("precision achieved");
                break;
            }
            if (maxSteps != null && step >= maxSteps) {
                System.out.println("steps achieved");
                break;
            }
            coeffs = newcoeffs;
        }
        final long stop = System.currentTimeMillis();
        System.out.println("Time is: " + String.valueOf(stop - start));
        return coeffs;
    }

    private double[] newCoeffs(double[] coeffs, double alpha) {
        double[] res = new double[coeffs.length];
        for (int i = 0; i < coeffs.length; i++) {
            res[i] = coeffs[i] + alpha / df.count() * sum(i);
        }
        return res;
    }

    private double[] newCoeffs(double[] coeffs) {
        double []res = newCoeffs(coeffs, alpha);
        while (rmse(res) > rmse(coeffs)) {
            alpha /= 2;
            System.out.println("alpha: " + alpha);
            res = newCoeffs(coeffs, alpha);
        }
        return res;
    }

    private double sum(int j) {
        return df.aggregate(0.0, new CalcGradient(this.coeffs, j), (a, b) -> a + b);
    }

    private double rmse(double[] coeffs) {
        return df.aggregate(0.0, new CalcRmse(coeffs), (a, b) -> a + b);
    }
}
