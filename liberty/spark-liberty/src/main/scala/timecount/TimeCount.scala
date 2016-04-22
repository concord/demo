package com.concord.timecount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

/**
 * Implement a computation that consumes a kafka topic and counts all
 * unique space delimited strings grouped by month and year, per
 * particular windowing duration and interval
 */
class TimeCountBenchmark(
  override val brokers: String, override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  private val windowLength: Duration = Seconds(10)
  private val slideInterval: Duration = Seconds(1)

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "TimeCount"
  override def streamLogic: Unit = {
    stream
      .flatMap(x => SimpleDateParser.parse(x._2) match {
        case Some(x) => Some((s"${x.month}-${x.year}", x.msg))
        case _ => None
      })
      .groupByKeyAndWindow(windowLength, slideInterval)
      .map(x => (x._1, x._2.toSet.size))
      .print
  }
}

object TimeCountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new TimeCountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
