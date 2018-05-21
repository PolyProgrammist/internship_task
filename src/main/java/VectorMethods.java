public class VectorMethods {
    /**
     * Dot product of two vectors.
     * The dot product of a and b is sum of a_i * b_i over all i
     * @param a First vector
     * @param b Second vector
     * @return The dot product of a and b
     */
    public static double dotProduct(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("dotProduct. Length of first vector is "
                    + "not equal to the length of the second vector");
        }
        double res = 0.0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }
}
