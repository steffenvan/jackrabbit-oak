package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CountMinSketchTest {

    private static final String WITH_DELIMITER = "(?=1$s)";

    @Test
    public void testDeserializeInvalidRow() {
        String invalid = "123 22 invalid 233";
        long[] res = CountMinSketch.deserialize(invalid, 4);
        assertArrayEquals(res, new long[4]);
    }

    @Test
    public void testDeserializeInvalidRowTooShort() {
        String invalid = "123 22";
        long[] res = CountMinSketch.deserialize(invalid, 4);
        assertArrayEquals(res, new long[4]);
    }

    @Test
    public void testColsTooSmall() {
        Throwable e = assertThrows(IllegalArgumentException.class,
                                   () -> new CountMinSketch(rows(5), cols(1)));

        String expected =
                "The number of columns must be a power of 2 and " + "larger " + "than 2. Got: " + 1;

        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testColsArgumentNotPowerOf2() {
        int cols = 7;
        Throwable e = assertThrows(IllegalArgumentException.class,
                                   () -> new CountMinSketch(rows(5), cols));

        String expected =
                "The number of columns must be a power of 2 and " + "larger " + "than 2. Got: " + cols;

        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testPrecision() {
        int[] data = getRandom();
        int numData = data.length;
        int rows = 5;
        int cols = 8;
        double epsilon = 2.0 / cols;
        double delta = 1 - 1 / Math.pow(2, rows);
        CountMinSketch sketch = new CountMinSketch(rows, cols);
        sketch.setSeed(0);

        // add to the CMS
        for (int datum : data) {
            sketch.add(Hash.hash64(datum));
        }

        // get the actual counts of each data point to compare with the
        // estimated one
        // from the CMS.
        Map<Integer, Integer> actualFrequencies = new HashMap<>();
        for (int num : data) {
            actualFrequencies.put(num,
                                  actualFrequencies.getOrDefault(num, 0) + 1);
        }

        int numErrors = 0;
        // check that the error ratio is within the specified bounds
        for (int num : data) {
            double errorRatio = ((double) sketch.estimateCount(
                    Hash.hash64(num)) - actualFrequencies.get(num)) / numData;
            System.out.println(errorRatio);
            if (errorRatio >= epsilon) {
                numErrors++;
            }
        }

        double correct = 1.0 - ((double) numErrors / actualFrequencies.size());
        System.out.println(correct);
        assertTrue(
                "delta not reached: required " + delta + ", reached " + correct,
                correct > delta);
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

    private int[] getRandom() {
        return IntStream.generate(() -> new Random().nextInt(100))
                        .limit(100)
                        .toArray();
    }

    @Test
    public void testSerialization() {
        int numData = 10;
        //		int[] data = getRandomData(numData);
        int[] data = getRandom();
        System.out.println(Arrays.toString(data));
        double epsilon = 0.001;
        double delta = 0.99;


        int rows = 5;
        int cols = 8;
        CountMinSketch sketch = new CountMinSketch(rows, cols);

        for (int num : data) {
            sketch.add(Hash.hash64(num));
        }

        long[][] expected = sketch.getData();

        String[] test = sketch.serialize();

        long[][] actual = new long[rows][cols];
        for (int i = 0; i < test.length; i++) {
            actual[i] = CountMinSketch.deserialize(test[i], cols);
        }

        for (long[] d : actual) {
            System.out.println(Arrays.toString(d));
        }

        for (int i = 0; i < expected.length; i++) {
            Assert.assertArrayEquals(expected[i], actual[i]);
        }
    }

    @Test
    public void testPrecisionWithStrings() {
        List<String> sameName = new ArrayList<>();
        sameName.add("rep:isAllow");
        sameName.add("ignoreData");
        sameName.add("jcr:isAbstract");
        sameName.add("collapsed");

        CountMinSketch cms = new CountMinSketch(5, 16);
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            for (String name : sameName) {
                long properHash = Hash.hash64(name.hashCode());
                if (seen.contains((long) name.hashCode()) || seen.contains(
                        properHash)) {
                    System.out.println("ERROR");
                }
                seen.add((long) name.hashCode());
                seen.add(properHash);
                cms.add(properHash);
            }
            seen.clear();
        }

        for (String name : sameName) {
            long properHash = Hash.hash64(name.hashCode());
            assertTrue(cms.estimateCount(properHash) <= 1000 * 1.01);
        }
    }

    private int rows(int r) {
        return r;
    }

    private int cols(int c) {
        return c;
    }
}
