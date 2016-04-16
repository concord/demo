package com.concord.count

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HLL

import scala.collection.mutable.{HashMap => MutableHashMap}

/** Implement a computation that consumes a kafka topic and estimated
  * all unique space delimited strings grouped by month and year
  */
class CountBenchmark(brokers: String, topics: Set[String])
    extends BenchmarkStreamContext[(String, Double)](brokers, topics) {
  private val allEstimates = MutableHashMap[String, HLL]()
  private val hllMonoid = new HyperLogLogMonoid(12)

  override def getApplicationName(): String = "Count"
  override def getCheckpointDirName(): String = "count"

  //override def streamLogic(): DStream[(String, Double)] = ???
    // /** Convert dstream to DStream[(key, value)] of valid logs */
    // val logs = stream
    //   .map(x => SimpleDateParser.parse(x._2) match {
    //     case Some(x) => Some((s"$x.month-$x.year", x.msg))
    //     case _ => None
    //   })
    //   .filter(!_.isEmpty)
    //   .map(_.get)

    // /** Create HLL with one item in it per entry in DStream
    //   * Then, merge all HLL's by key
    //   * Map over one last time to output results as HLL est size
    //   */
    // val approximations = logs
    //   .map(kv => (kv._1, hllMonoid.create(kv._2.getBytes)))
    //   .reduceByKey(_ + _)
    //   .map(kv => (kv._1, kv._2.estimatedSize))

    // // Not correct to return this... this only performs an approx per
    // // Spark streaming interval. We need to keep state so that this is
    // // merged with a global Map[String, HLL] to see total state over time
    // approximations
    //}
}

object CountBenchmark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: CountBenchmark <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |""".stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    new CountBenchmark(brokers, topicsSet).start()
  }
}
