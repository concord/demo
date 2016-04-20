package com.concord.contexts

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf

/**
 * Use this StreamContext to ingest data from a set of kafka brokers on a
 * given topic set. A bunch of stuff is hardcoded for this demo, consider
 * making this configurable. Just override the unimplemented methods with
 * your stream processing logic.
 */
trait BenchmarkStreamContext {
  def brokers: String = ???
  def topics: Set[String] = ???
  lazy val sparkConf = new SparkConf()
    .setAppName(applicationName)
    .set("spark.mesos.executor.home", "/usr/lib/spark")
    .set("spark.default.parallelism", "120")
    //.set("spark.streaming.backpressure.enabled", "true") // Throttle rate
    .set("spark.streaming.kafka.maxRatePerPartition", streamingRate.toString)

  private lazy val ssc = new StreamingContext(sparkConf, batchInterval)
  ssc.checkpoint("/tmp/concord_spark_" + applicationName.toLowerCase)

  private lazy val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "auto.offset.reset" -> "smallest")

  lazy val stream = KafkaUtils.createDirectStream[String,
    String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  def streamingRate: Int = ???
  def batchInterval: Duration = ???
  def applicationName: String = ???
  def streamLogic: Unit = ???
  def start(): Unit = {
    streamLogic
    ssc.start()
    ssc.awaitTermination()
  }
}
