package io.concord.demo.liberty.spark

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._


class Deduplication(
  override val brokers: String, override val topics: Set[String])
    extends BenchmarkStreamContext {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "TimeCount"
  override def streamLogic: Unit = {
    // val data = lines map { line => (LineParser(line), 1) }
    // val uniques = data.reduceByKey(_ + _)
    // val tsUpFront = uniques map { line => (line._1.get.tsSecs, line._1.get.fullLine) }
    // //Maybe not needed depending on equals & hashcode of LogLine?
    // val sorted = tsUpFront.transform(rdd => rdd.sortByKey(true))

  }
}


object Deduplication extends App {
  val argHelper = new SparkArgHelper(args)
  new Deduplication(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
