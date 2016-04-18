package com.concord.contexts

import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf

import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }

object BenchmarkStreamContext extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}

/**
 * Use this StreamContext to ingest data from a set of kafka brokers on a
 * given topic set. A bunch of stuff is hardcoded for this demo, consider
 * making this configurable. Just override the unimplemented methods with
 * your stream processing logic.
 */
class BenchmarkStreamContext[K, V](
  brokers: String,
  topics: Set[String]) {

  lazy val sparkConf = new SparkConf()
    .setAppName(applicationName)
    .set("spark.mesos.executor.home", "/usr/lib/spark")
    .set("spark.default.parallelism", "120")
    .set("spark.streaming.kafka.maxRatePerPartition", streamingRate.toString)

  //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //.set("spark.kryoserializer.buffer.mb","24")

  private lazy val ssc = new StreamingContext(sparkConf, batchInterval)
  ssc.checkpoint("/tmp/__" + applicationName.toLowerCase)

  private lazy val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "auto.offset.reset" -> "smallest")

  lazy val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  def streamingRate: Int = ???
  def batchInterval: Duration = ???
  def applicationName: String = ???
  def streamLogic: Unit = ???

  def start(): Unit = {
    BenchmarkStreamContext.setStreamingLogLevels()
    streamLogic
    ssc.start()
    ssc.awaitTermination()
  }
}
