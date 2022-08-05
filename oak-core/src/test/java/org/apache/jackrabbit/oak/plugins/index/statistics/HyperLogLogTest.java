package org.apache.jackrabbit.oak.plugins.index.statistics;

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
		double maxError = 1.3 / Math.sqrt(m);
		double error = (double) estimated / actual;
		System.out.println(error);
		Assert.assertTrue("maxerror: " + maxError + "got: " + error, error <= maxError);
		System.out.println(actualUniqueElements.size());
		System.out.println(hll.estimate());
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

		byte[] expected = hll.getCounts();
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
}
