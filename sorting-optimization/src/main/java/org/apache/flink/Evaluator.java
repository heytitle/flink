package org.apache.flink;

import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;


//import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Hello world!
 *
 */
public class Evaluator
{

//	@Benchmark
//	public int shortHandCondition() {
//		if( 1 > 2 && true ) {
//			return 5;
//		}
//		return 2;
//	}
//
//	@Benchmark
//	public int normalCondition() {
//		if( 1 > 2 & true ) {
//			return 5;
//		}
//		return 2;
//	}

	public static void main( String[] args ) throws RunnerException {
		System.out.println("Running Sorting Evaluation");
		NormalizedKeySorter b = null;

		Options opt = new OptionsBuilder()
			.include(SortEvaluation.class.getSimpleName())
			.forks(1)
			.build();

		new Runner(opt).run();
	}

}
