package com.concord.bucketcount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{ LogParser, SparkArgHelper, SimpleDateParser }
import org.apache.spark.streaming.{ Duration, Seconds }

import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Queue => MutableQueue}

class BucketCountBenchmark(
  override val brokers: String, override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  private val windowLength: Int = 1000000
  private val slideInterval: Int = 100000

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 700 // Hasn't been calculated
  override def applicationName: String = "BucketCount"

  override def streamLogic: Unit = {
    import com.concord.utils.EnrichedStreams._

    implicit val queue = MutableQueue[RDD[(String, String)]]()

    stream
      /** Strip bad logs, return tuple of newly built (K, V) */
      .flatMap(x => SimpleDateParser.parse(x._2) match {
        case Some(x) => Some((s"${x.month}-${x.year}", x.msg))
        case _ => None
      })
      /** Group logs by month-year key */
      //.groupByKey()
      /** Break stream into discrete chunks 'overlapping' windows */
      .countingWindow(windowLength, slideInterval)
      .map((x) => x.size)
      .foreachRDD(rdd => {
        println("RDD count: " + rdd.count)
        rdd.foreach(size => {
          println(s"There are ${size} unique records in this bucket")
        })
     })
  }
}

object BucketCountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new BucketCountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
