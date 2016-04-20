package com.concord.count

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.twitter.algebird.{HyperLogLogMonoid, HyperLogLog, HLL}

/** Implement a computation that consumes a kafka topic and estimated
  * all unique space delimited strings grouped by month and year
  */
class CountBenchmark(
  override val brokers: String, override val topics: Set[String])
    extends BenchmarkStreamContext {
  override def batchInterval: Duration =  Seconds(1)
  override def streamingRate: Int = 10000
  override def applicationName: String = "Count"
  override def streamLogic: Unit = {
    /** Cannot exist in class scope unless custom serializer is created */
    val globalHll = new HyperLogLogMonoid(12)

    /** Convert dstream to DStream[(date, HLL)] of valid logs, then
      * reduce all values under the same month/year into one HLL */
    val logs = stream
      .flatMap(x => SimpleDateParser.parse(x._2) match {
        case Some(x) =>
          val hllMonoid = new HyperLogLogMonoid(12)
          Some(s"${x.month}-${x.year}", hllMonoid.create(x.msg.getBytes))
        case _ => None
      })
      .reduceByKey(_ + _)

    /** Aggregate batch into global HLL instance and print results */
    var h = globalHll.zero
    logs.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        h += partial._2
        println(s"Data for key: ${partial._1}")
        println(s"Approximate distinct counts: ${globalHll.estimateSize(h).toInt}")
      }
    })
  }
}

object CountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new CountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
