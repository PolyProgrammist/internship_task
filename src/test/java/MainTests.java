import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MainTests {
    public static JavaSparkContext sc;

    static {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Simple").setMaster("local");
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void dotProduct() {
        assertEquals(146.0, VectorMethods.dotProduct(new double[]{5, 6, 7}, new double[]{7, 8, 9}));
        assertEquals(-17.0, VectorMethods.dotProduct(new double[]{5, 8}, new double[]{3, -4}));
        assertEquals(50.0, VectorMethods.dotProduct(new double[]{2, 6, 0}, new double[]{4, 7, 9}));
        assertEquals(0.0, VectorMethods.dotProduct(new double[] {}, new double[] {}));
        assertEquals(0.0, VectorMethods.dotProduct(new double[] {0, 0, 0}, new double[] {0, 0, 0}));
        assertEquals(2.5, VectorMethods.dotProduct(new double[] {0.5}, new double[] {5}));
    }

    @Test
    public void dotProductException() throws IllegalArgumentException {
        assertThrows(IllegalArgumentException.class, () -> {
            VectorMethods.dotProduct(new double[]{5, 6}, new double[]{7, 8, 9});
        });
    }

    @Test
    public void calcGradient() {
        Sample s = new Sample(new double[] {4, 1, 3}, 5.0);
        CalcGradient cg = new CalcGradient(new double[] {1, 3, 5}, 2);
        assertEquals(Double.valueOf(-49.0), cg.call(2.0, s));
    }

    @Test
    public void calcRmse() {
        Sample s = new Sample(new double[] {4, 1, 3}, 5.0);
        CalcRmse cg = new CalcRmse(new double[] {1, 3, 5});
        assertEquals(Double.valueOf(291.0), cg.call(2.0, s));
    }
}
