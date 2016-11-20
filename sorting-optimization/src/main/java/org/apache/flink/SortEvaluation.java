package org.apache.flink;

/**
 * Created by heytitle on 11/20/16.
 */

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

// See also https://github.com/SoatGroup/benchmark-with-jmh/tree/06fd73b6ab1b7eb59da9249d37e74002446f284f/jmh-fixtures/src/main/java/fr/soat

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10, time=500, timeUnit=TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time=500, timeUnit=TimeUnit.MILLISECONDS)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SortEvaluation {



	@State(Scope.Benchmark)
	public static class ArrayContainer {

		@Param({ "10000"})
		int arraySize;

		// initial unsorted array
		int [] suffledArray;
		// system date copy
		int [] arrayToSort;

		@Setup(Level.Trial)
		public void initArray() {
			// create a shuffled array of int
			suffledArray = new int[arraySize];
			for (int i = 0; i < arraySize; i++) {
				suffledArray[i] = new Random().nextInt(1000);
			}
		}

		@Setup(Level.Invocation)
		public void makeArrayCopy() {
			// copy shuffled array to reinit the array to sort
			arrayToSort = suffledArray.clone();
		}

		int[] getArrayToSort() {
			return arrayToSort;
		}
	}

	@Benchmark
	public int[] baseline(ArrayContainer d) {
		// sort copy of shuffled array
		Arrays.sort(d.getArrayToSort());
		// array is now sorted !
		return d.getArrayToSort();
	}
}
