package org.apache.flink.examples.scala

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by heytitle on 1/13/17.
  */
object JoinSomething {

  val warmup = 2
  val totalRun = 10

  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val sorterName = "HandWrittenSorter"

  def main(args: Array[String]) {


    println(s"Collected from ${totalRun} runs with ${warmup} warmup")

    val basket = List("100000", "1000000", "10000000", "100000000") map { size =>
      val results = (0 until totalRun + warmup)
        .map( run(size, _) )
        .toList

      val (avg, std) = computeStats( results.drop(warmup).map(_._1) )


      val output = results.head._2.sortBy(_._1._1).mkString("\n")
      Files.write(Paths.get(s"result-${sorterName}-${size}"), output.getBytes(StandardCharsets.UTF_8))
      (size, avg, std )
    }

    val summaries = basket map { r =>
      val size = r._1
      val avg  = r._2
      val std  = r._3
      s"Average execution time for ${size} records : \t ${avg} ± ${std} ms"
    }

    println(s"------ ${sorterName}'s stats --------")
    println( summaries.mkString("\n") )

  }

  def run(fileSize: String, round:Int): (Long, Seq[((Long, Int), (Long, Int))]) = {

    env.getConfig.disableSysoutLogging()
    env.getConfig.enableObjectReuse()

    val numbers = env.readTextFile(s"/Users/heytitle/projects/apache-flink/random-numbers-${fileSize}.txt")
      .map { s: String => (s.toLong, 1) }

    val numbers2: DataSet[Tuple2[Long, Int]] = env.fromCollection(List(8, 9, 3, 5, 1, 2, 4, 0, 7))
      .map {
        (_, 1)
      }
    val joined = numbers.join(numbers2, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0);

    val res  = joined.collect()

    return (env.getLastJobExecutionResult.getNetRuntime, res)
  }

  def computeStats( l: List[Long] ) : (Double, Double) = {
    val mean = l.sum / l.size

    val variance = l.map{ n => math.pow(n - mean,2) }.sum / ( l.size - 1 )

    (mean, math.sqrt(variance) )
  }

}

