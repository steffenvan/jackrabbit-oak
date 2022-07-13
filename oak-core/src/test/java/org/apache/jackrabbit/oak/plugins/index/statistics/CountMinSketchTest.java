package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountMinSketchTest {

    @Test
    public void testAdd() {
        CountMinSketch sketch = new CountMinSketch(rows(3), cols(50));
        sketch.setSeed(0);

        Random r = new Random();
        int[] data = new int[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = r.nextInt();
            System.out.println(data[i]);
        }

        for (int datum : data) {
            sketch.add(Hash.hash64(datum));
        }
        System.out.println(Arrays.deepToString(sketch.getItems()));



    }

    private static int rows(int r) {
        return r;
    }

    private static int cols(int c) {
        return c;
    }
}
