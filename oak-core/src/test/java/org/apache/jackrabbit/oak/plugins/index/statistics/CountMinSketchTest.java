package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class CountMinSketchTest {

	private static final String WITH_DELIMITER = "(?=1$s)";

	// TODO: Improve tests
	@Test
	public void testPrecision() {
		int numData = 1000000;
		int[] data = getRandomData(numData);
		int rows = 7;
		int cols = 32;
		double epsilon = 2.0 / cols;
		double delta = 1 - 1 / Math.pow(2, rows);
		CountMinSketch sketch = new CountMinSketch(rows, cols);
		sketch.setSeed(0);

		// add to the CMS
		for (int datum : data) {
			sketch.add(Hash.hash64(datum));
		}

		// get the actual counts of each data point to compare with the estimated one
		// from the CMS.
		Map<Integer, Integer> actualFrequencies = new HashMap<>();
		for (int num : data) {
			actualFrequencies.put(num, actualFrequencies.getOrDefault(num, 0) + 1);
		}

		int numErrors = 0;
		// check that the error ratio is within the specified bounds
		for (int num : data) {
			double errorRatio = ((double) sketch.estimateCount(Hash.hash64(num)) - actualFrequencies.get(num))
					/ numData;
			if (errorRatio >= epsilon) {
				numErrors++;
			}
		}

		double correct = 1.0 - ((double) numErrors / actualFrequencies.size());
		assertTrue("delta not reached: required " + delta + ", reached " + correct, correct > delta);
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
	public void testSerialization() {
		int numData = 10;
		int[] data = getRandomData(numData);

		double epsilon = 0.001;
		double delta = 0.99;

		int rows = 3;
		int cols = 10;
		CountMinSketch sketch = new CountMinSketch(rows, cols);

		for (int num : data) {
			sketch.add(Hash.hash64(num));
		}

		long[][] expected = sketch.getData();

		String[] test = sketch.serialize();

		long[][] actual = new long[rows][cols];
		for (int i = 0; i < test.length; i++) {
			actual[i] = CountMinSketch.deserialize(test[i]);
		}

		for (long[] d : actual) {
			System.out.println(Arrays.toString(d));
		}

		for (int i = 0; i < expected.length; i++) {
			Assert.assertArrayEquals(expected[i], actual[i]);
		}
	}

	private int rows(int r) {
		return r;
	}

	private int cols(int c) {
		return c;
	}

	private double epsilon(double x) {
		return x;
	}

	private double delta(double x) {
		return x;
	}
}
