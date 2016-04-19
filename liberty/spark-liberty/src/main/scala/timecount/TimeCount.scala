package com.concord.timecount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

/** Implement a computation that consumes a kafka topic and counts all
  * unique space delimited strings grouped by month and year, per
  * particular windowing duration and interval
  */
class TimeCountBenchmark(
  override val brokers: String, override val topics: Set[String])
    extends BenchmarkStreamContext {
  private val windowLength: Duration = Seconds(10)
  private val slideInterval: Duration = Seconds(1)

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "TimeCount"
  override def streamLogic: Unit = {
    /** Parse List[DStream[(K,V)]] and returned filtered valid logs  */
    val logStream = stream
      .map(x => SimpleDateParser.parse(x._2) match {
        case Some(x) => Some((s"$x.month-$x.year", x.msg))
        case _ => None
      })
      .filter(!_.isEmpty)
      .map(_.get)

    /** Grouping operation creates DStream[K, Iterable[V]] split accross windows,
      * Then map value collection into a set in order to get a count of unique
      * items in the iterable
      */
    val uniqueWindowGroups = logStream
      .groupByKeyAndWindow(windowLength, slideInterval)
      .map(x => (x._1, x._2.toSet.size))
  }
}

object TimeCountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new TimeCountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  )
}
