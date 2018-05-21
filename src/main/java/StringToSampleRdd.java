import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class StringToSampleRdd {
    /**
     * Converts the String RDD to Sample RDD to work with GradientDescent.
     * @param data Source JavaRDD
     * @return Resulting RDD to work with GradientDescent.
     */
    public static JavaRDD<Sample> convert(JavaRDD<String> data) {
        String header = data.first();
        data = data.filter(s -> !s.equals(header));
        JavaRDD<Sample> df = data.map((Function<String, Sample>) line -> {
            List<String> fields = Arrays.asList(line.split(","));
            List<Double> lx = fields.stream().map(Double::valueOf).collect(Collectors.toList());
            Double y = lx.get(lx.size() - 1);
            lx.remove(lx.size() - 1);
            lx.add(1.0);
            double[] x = lx.stream().mapToDouble(Double::doubleValue).toArray();
            return new Sample(x, y);
        });
        return df;
    }
}
