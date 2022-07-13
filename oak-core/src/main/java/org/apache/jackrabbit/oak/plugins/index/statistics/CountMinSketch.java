package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.Arrays;
import java.util.Random;

public class CountMinSketch implements FrequencyCounter{
    private final long[][] items;
    private final int rows;
    private final int cols;
    private final int shift;

    private long count;


    private static final Random RANDOM = new Random();


    public CountMinSketch(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
        this.shift = Integer.bitCount(rows - 1);
        items = new long[rows][cols];
    }

    void setSeed(int seed) {
        RANDOM.setSeed(seed);
    }

    public long[][] getItems() {
        return items;
    }

    @Override
    public void add(long hash) {
        count++;
        for (int i = 0; i < rows; i++) {
            int col = (int) hash & (cols - 1);
            items[i][col]++;
            hash >>>= shift;
        }
    }


    @Override
    public long estimateCount(long hash) {
        long currMin = Long.MAX_VALUE;
        for (int i = 0; i < rows; i++) {
            currMin = Math.min(currMin, items[i][(int) hash & (cols - 1)]);
            hash >>>= shift;
        }

        return currMin;
    }
}
