import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate();
        SparkContext sc = spark.sparkContext();
        JavaRDD<String> data = spark.read().textFile("learn.csv").javaRDD();
        String header = data.first();
        data = data.filter(s -> !s.equals(header));
//        Dataset<Row> samples = spark.read().format("csv").option("header", "true").load("learn.csv");
        JavaRDD<Sample> df = data.map((Function<String, Sample>) line -> {
                    List<String> fields = Arrays.asList(line.split(","));
                    List<Double> lx = fields.stream().map(Double::valueOf).collect(Collectors.toList());
                    Double y = lx.get(lx.size() - 1);
                    lx.remove(lx.size() - 1);
                    lx.add(1.0);
                    double[] x = lx.stream().mapToDouble(Double::doubleValue).toArray();
                    return new Sample(x, y);
        });
        GradientDescent gd = new GradientDescent(df);
        gd.GetCoeffs();
    }
}