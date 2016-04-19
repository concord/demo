package com.concord.count

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HLL

// class SHLL extends HLL with Serializable {
//   override def toString = toBytes(this)
// }

/** Implement a computation that consumes a kafka topic and estimated
  * all unique space delimited strings grouped by month and year
  */
class CountBenchmark(
  override val brokers: String, override val topics: Set[String])
    extends BenchmarkStreamContext {
  private val hllMonoid = new HyperLogLogMonoid(12)

  override def batchInterval: Duration =  Seconds(1)
  override def streamingRate: Int = 2000
  override def applicationName: String = "Count"
  override def streamLogic: Unit = {
    /** Convert dstream to DStream[(date, (msg, HLL))] of valid logs */
    val logs = stream
      .map(x => SimpleDateParser.parse(x._2) match {
        case Some(x) =>
          Some(s"$x.month-$x.year", hllMonoid.create(x.msg.getBytes))
        case _ => None
      })
      .filter(!_.isEmpty)
      .map(_.get)

    /** Defines a function to shuffle state accross DStream[(String, HLL)] */
    val sMap = (date: String, est: Option[HLL], state: State[HLL]) => {
      val sum = est.getOrElse(hllMonoid.zero) + state.getOption.getOrElse(hllMonoid.zero)
      val output = (date, sum)
      state.update(sum)
      output
    }

    // Stream of data to commit
    val estCounts = logs
      .mapWithState(StateSpec.function(sMap))
      .map(kv => (kv._1, kv._2.estimatedSize))
      .print
  }
}

object CountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new CountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
