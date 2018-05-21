import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    /**
     * Showing the task implementation.
     * @param args Command Line Arguments
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        GradientDescent gd = new GradientDescent(
                StringToSampleRdd.convert(
                        sc.textFile("testdata/learn.csv")
                )
        );
        gd.getCoeffs();
    }
}