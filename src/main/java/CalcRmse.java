import org.apache.spark.api.java.function.Function2;

class CalcRmse implements Function2<Double, Sample, Double> {
    private final double[] coeffs;

    CalcRmse(double[] coeffs) {
        this.coeffs = coeffs;

    }

    @Override
    public Double call(Double d, Sample s) {
        double t = VectorMethods.dotProduct(s.row, coeffs);
        t = s.val - t;
        return d + t * t;
    }
}
