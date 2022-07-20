package org.apache.jackrabbit.oak.plugins.index.statistics;

public class HyperLogLog implements CardinalityEstimator {

    private final int m;
    private byte[] counts;
    private final double am;

    public HyperLogLog(int m) {
        this.m = m;
        this.counts = new byte[m];
        switch (m) {
            case 32:
                this.am = 0.697;
                break;
            case 64:
                this.am = 0.709;
                break;
            default:
                this.am = 0.7213 / (1.0 + 1.079 / m);
        }
    }

    @Override
    public void add(long hash) {
        int i = (int) (hash & (m - 1));
        counts[i] = (byte) Math.max(counts[i], 1 + Long.numberOfLeadingZeros(hash));
    }

    @Override
    public long estimate() {
        int numZeroes = 0;
        double sum = 0.0;
        for (int count : counts) {
            sum += 1.0 / (1 << count);
            if (count == 0) {
                numZeroes++;
            }
        }
        double estimate = am * m * m / sum;

        if (estimate <= 5 * m && numZeroes > 0) {
            return Math.round(linearCount(numZeroes));
        } else {
            return Math.round(estimate);
        }
    }

    private double linearCount(int numZeroes) {
        return m * Math.log(1.0 * m / numZeroes);
    }
}
