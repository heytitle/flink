package org.apache.flink.examples.scala

import java.io.DataInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

import org.apache.flink.api.common.io.OutputFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by heytitle on 1/13/17.
  */
object SortEvaluation {

  val warmup = 2
  val totalRun = 10

  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val sorterName = "HandWrittenSorter"

  val writeOutput = true

  def main(args: Array[String]) {


    println(s"Collected from ${totalRun} runs with ${warmup} warmup")

    val basket = List("100000") map { size =>
      val results = (0 until totalRun + warmup)
        .map( run(size, _) )
        .toList

      val (avg, std) = computeStats( results.drop(warmup) )

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

  def run(fileSize: String, round:Int): Long = {

    env.getConfig.disableSysoutLogging()
    env.getConfig.enableObjectReuse()

    val numbers = env.readTextFile(s"/Users/heytitle/projects/apache-flink/random-numbers-${fileSize}.txt")
      .map { s: String => (s.toLong, 1) }

    val res  = numbers.sortPartition(0, Order.ASCENDING )


    if(writeOutput) {
      val output = res.collect().mkString("\n")
      Files.write(Paths.get(s"result-${sorterName}-${fileSize}"), output.getBytes(StandardCharsets.UTF_8))
    }
    else {
      res.output( new DiscardingOutputFormat[(Long, Int)])
      env.execute()
    }

    return env.getLastJobExecutionResult.getNetRuntime
  }

  def computeStats( l: List[Long] ) : (Double, Double) = {
    val mean = l.sum / l.size

    val variance = l.map{ n => math.pow(n - mean,2) }.sum / ( l.size - 1 )

    (mean, math.sqrt(variance) )
  }

}

