import org.apache.spark.api.java.function.Function2;

class CalcGradient implements Function2<Double, Sample, Double> {
    private final double[] coeffs;
    private final int componentNumber;

    CalcGradient(double[] coeffs, int componentNumber) {
        this.coeffs = coeffs;
        this.componentNumber = componentNumber;
    }

    @Override
    public Double call(Double d, Sample s) {
        double t = VectorMethods.dotProduct(s.row, coeffs);
        return d + (s.val - t) * s.row[componentNumber];
    }
}
