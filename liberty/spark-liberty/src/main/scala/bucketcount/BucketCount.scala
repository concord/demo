package com.concord.bucketcount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{ LogParser, SparkArgHelper, SimpleDateParser }
import org.apache.spark.streaming.{ Duration, Seconds }

import org.apache.spark.streaming.dstream._

class BucketCountBenchmark(
  override val brokers: String, override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  private val windowLength: Int = 1000000
  private val slideInterval: Int = 100000

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750 // Hasn't been calculated
  override def applicationName: String = "BucketCount"

  override def streamLogic: Unit = {
    import com.concord.utils.EnrichedStreams._

    stream
      .flatMap(x => SimpleDateParser.parse(x._2) match {
        case Some(x) => Some((s"${x.month}-${x.year}", x.msg))
        case _  => None
      })
      .groupByKey()
      .countingWindow(windowLength, slideInterval)
      .map((x) => x._2.toSet.size)
      .foreachRDD(rdd => {
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
