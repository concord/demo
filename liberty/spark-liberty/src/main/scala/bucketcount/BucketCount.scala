package com.concord.bucketcount

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

class BucketCountBenchmark(
  override val brokers: String, override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  /** Local constants */
  private val windowLimit: Int = 100000
  private val windowInterval: Int = 100000

  override def batchInterval: Duration = ???
  override def streamingRate: Int = ???
  override def applicationName: String = "BucketCount"
  override def streamLogic: Unit = {
    // Not yet implemented
  }
}

object BucketCountBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new BucketCountBenchmark(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
