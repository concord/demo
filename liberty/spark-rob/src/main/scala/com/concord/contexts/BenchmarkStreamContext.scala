package com.concord.contexts

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkConf

import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.serializer.KryoRegistrator

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

/** Use this StreamContext to ingest data from a set of kafka brokers on a
  * given topic set. A bunch of stuff is hardcoded for this demo, consider
  * making this configurable. Just override the unimplemented methods with
  * your stream processing logic.
  */
class BenchmarkStreamContext[T](
  brokers: String,
  topics: Set[String]) {

  private lazy val sparkConf = new SparkConf()
    .setAppName(getApplicationName())
    .set("spark.mesos.executor.home", "/usr/lib/spark")
    .set("spark.default.parallelism", "120")
    .set("spark.streaming.kafka.maxRatePerPartition", "700")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.kryoserializer.buffer.mb","24")

  private lazy val ssc = new StreamingContext(sparkConf, Milliseconds(500))
  ssc.checkpoint("/tmp/" + getCheckpointDirName())

  private lazy val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "auto.offset.reset" -> "smallest"
  )

  val stream =  KafkaUtils.createDirectStream[String,
      String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  def getApplicationName(): String = ???
  def getCheckpointDirName(): String = ???
  def streamLogic(): DStream[T] = ???

  def start(): Unit = {
    BenchmarkStreamContext.setStreamingLogLevels()
    streamLogic().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
