package com.concord.timecount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

/** Implement a computation that consumes a kafka topic and counts all
  * unique space delimited strings grouped by month and year, per
  * particular windowing duration and interval
  */
class TimeCountBenchmark(brokers: String,
                         topics: Set[String],
                         length: Duration,
                         interval: Duration)
    extends BenchmarkStreamContext[(String, Int)](brokers, topics) {
  override def getApplicationName(): String = "TimeCount"
  override def getCheckpointDirName(): String = "time_count"

  override def streamLogic(): DStream[(String, Int)] = {
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
      .groupByKeyAndWindow(length, interval)
      .map(x => (x._1, x._2.toSet.size))

    uniqueWindowGroups
  }
}

object TimeCountBenchmark {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
        |Usage: TimeCountBenchmark <brokers> <topics> <window length> <slide interval>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <window length> time in ms that records will be collected into window
        |  <slide interval> time in ms until next window is opened
        |""".stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics, length, interval) = args
    val topicsSet = topics.split(",").toSet
    new TimeCountBenchmark(brokers, topicsSet,
      Milliseconds(length.toInt), Milliseconds(interval.toInt)).start()
  }
}
