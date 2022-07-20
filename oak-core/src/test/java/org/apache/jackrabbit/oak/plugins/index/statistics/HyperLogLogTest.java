package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class HyperLogLogTest {

    @Test
    public void testAdd() {
        int m = 64;
        HyperLogLog hll = new HyperLogLog(m);
        int numData = 10_000_000;
        int[] randomData = getRandomData(numData);
//        System.out.println(Arrays.toString(randomData));

        Set<Integer> actualUniqueElements = Arrays.stream(randomData).boxed().collect(Collectors.toSet());

        for (int num : randomData) {
            long hash = Hash.hash64(num);
            hll.add(hash);
        }
        int actual = actualUniqueElements.size();
        long estimated = hll.estimate();
        System.out.println((double) estimated / actual);
        System.out.println(actualUniqueElements.size());
        System.out.println(hll.estimate());


    }

    private int[] getRandomData(int numData) {
        Random r = new Random();
        int[] data = new int[numData];
        int maxNum = 20;
        for (int i = 0; i < data.length; i++) {
            int num = r.nextInt(maxNum);
            data[i] = r.nextInt(1 << num);
        }
        return data;
    }
}
