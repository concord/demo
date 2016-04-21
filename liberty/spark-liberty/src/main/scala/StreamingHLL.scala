package com.concord.demo.liberty.spark

import org.apache.spark;
import spark.streaming.StreamingContext._
import spark.streaming.{ Seconds, StreamingContext }
import spark.SparkContext._
import spark.storage.StorageLevel
import com.twitter.algebird.HyperLogLog._
import com.twitter.algebird._
import spark.streaming.Duration

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SimpleDateParser
import com.concord.utils.SparkArgHelper

class StreamingHLL(
  override val brokers: String, override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "TestAlexHyperLogLog"
  override def streamLogic: Unit = {
    /**
     * Error is about 1.04/sqrt(2^{bits}), so you want something
     * like 12 bits for 1% error which means each HLLInstance is
     *  about 2^{12} = 4kb per instance.
     */
    val globalHll = new HyperLogLogMonoid(12)
    var globalTotalParsedLines: Int = 0
    var globalTotalAttemptedReads: Int = 0
    val lines = stream.flatMap(x => SimpleDateParser.parse(x._2) match {
      case Some(x) =>
        globalTotalAttemptedReads += 1;
        globalTotalParsedLines += 1;
        Some(x)
      case _ =>
        globalTotalAttemptedReads += 1;
        None
    })
    /**
     *  return the number of uniq lines ?
     */
    val approxLines = lines.mapPartitions(lines => {
      val hll = new HyperLogLogMonoid(12)
      lines.map(line => hll(line.msg.getBytes))
    }).reduce(_ + _)
    /**
     *  print the stats
     */
    var h = globalHll.zero
    approxLines.foreach(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        h += partial
        println(s"Approx distinct this batch: ${partial.estimatedSize.toInt}")
        println(s"Approx distinct overall: ${globalHll.estimateSize(h).toInt}")
        println(s"Total lines parsed: ${globalTotalParsedLines}")
        println(s"Total lines attempted: ${globalTotalAttemptedReads}")
      }
    })
  }
}
object StreamingHLL extends App {
  val argHelper = new SparkArgHelper(args)
  new StreamingHLL(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
