package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class HyperLogLogTest {

	@Test
	public void testAdd() {
		int m = 64;

		HyperLogLog hll = new HyperLogLog(m);
		int numData = 10000;
		int[] randomData = getRandomData(numData);

		Set<Integer> actualUniqueElements = Arrays.stream(randomData).boxed().collect(Collectors.toSet());

		for (int num : randomData) {
			long hash = Hash.hash64(num);
			hll.add(hash);
		}

		int actual = actualUniqueElements.size();
		long estimated = hll.estimate();
		double maxError = 2 * (1.3 / Math.sqrt(m));
		double error = Math.abs(((double) estimated / actual) - 1);
		// System.out.println(error);
		Assert.assertTrue("maxerror: " + maxError + " got: " + error, error <= maxError);
		// System.out.println(actualUniqueElements.size());
		// System.out.println(hll.estimate());
	}

	@Test
	public void testSerialization() {
		int m = 64;

		HyperLogLog hll = new HyperLogLog(m);
		int numData = 10;
		int[] randomData = getRandomData(numData);
		for (int num : randomData) {
			long hash = Hash.hash64(num);
			hll.add(hash);
		}

		byte[] expected = hll.getCounters();
		String s = hll.serialize();
		byte[] actual = hll.deserialize(s);
		Assert.assertArrayEquals(expected, actual);

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
	
	
	
    @Test
    public void test() {
        int testCount = 50;
        double avg = Math.sqrt(averageOverRange(30_000, testCount, false, 2));
        double min = 15, max = 16;
        // System.out.println(type + " expected " + min + ".." + max + " got " + avg);
        assertTrue("expected " + min + ".." + max + " got " + avg, min < avg && avg < max);
    }

    private static double averageOverRange(long maxSize, int testCount, boolean debug, double exponent) {
        double sum = 0;
        int count = 0;
        for (long size = 1; size <= 20; size++) {
            sum += test(size, testCount, debug, exponent);
            count++;
        }
        for (long size = 22; size <= 300; size += size / 5) {
            sum += test(size, testCount, debug, exponent);
            count++;
        }
        for (long size = 400; size <= maxSize; size *= 2) {
            sum += test(size, testCount, debug, exponent);
            count++;
        }
        return sum / count;
    }

    private static double test(long size, int testCount, boolean debug, double exponent) {
        long x = 0;
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        long ns = System.nanoTime();
        double sumSquareError = 0;
        double sum = 0;
        double sumFirst = 0;
        int repeat = 10;
        int runs = 2;
        for (int test = 0; test < testCount; test++) {
            CardinalityEstimator est = new HyperLogLog(32);
            long baseX = x;
            for (int i = 0; i < size; i++) {
                est.add(Hash.hash64(x));
                x++;
            }
            long e = est.estimate();
            sum += e;
            min = Math.min(min, e);
            max = Math.max(max, e);
            long error = e - size;
            sumSquareError += error * error;
            sumFirst += e;
            for (int add = 0; add < repeat; add++) {
                long x2 = baseX;
                for (int i = 0; i < size; i++) {
                    est.add(Hash.hash64(x2));
                    x2++;
                }
            }
            e = est.estimate();
            sum += e;
            min = Math.min(min, e);
            max = Math.max(max, e);
            error = e - size;
            sumSquareError += error * error;
        }
        ns = System.nanoTime() - ns;
        long nsPerItem = ns / testCount / runs / (1 + repeat) / size;
        double stdDev = Math.sqrt(sumSquareError / testCount / runs);
        double relStdDevP = stdDev / size * 100;
        int biasFirstP = (int) (100 * (sumFirst / testCount / size) - 100);
        int biasP = (int) (100 * (sum / testCount / runs / size) - 100);
        if (debug) {
            System.out.println("size " + size + " relStdDev% " + (int) relStdDevP + " range " + min + ".." + max
                    + " testCount " + testCount + " biasFirst% " + biasFirstP + " bias% " + biasP + " avg "
                    + (sum / testCount / runs) + " time " + nsPerItem);
        }
        // we try to reduce the relStdDevP, make sure there are no large values
        // (trying to reduce sumSquareError directly
        // would mean we care more about larger sets, but we don't)
        return Math.pow(relStdDevP, exponent);
    }
}
