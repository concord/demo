package com.concord.timematch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

class TimeMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  override val brokers: String,
  override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  /** Constants */
  private val windowLength: Duration = Seconds(10)
  private val slideInterval: Duration = Seconds(10)

  /** Overidden constants and impl */
  override def confParams: List[(String, String)] =
    List(("spark.cassandra.connection.host", cassandraHosts))
  override def batchInterval: Duration = Seconds(10)
  override def streamingRate: Int = 15000
  override def applicationName: String = "TimeMatch"
  override def streamLogic: Unit = {
    stream
      .flatMap(x => LogParser.parse(x._2) match {
        case Some(x) if (x.msg.contains("IRQ")) => Some(x.buildKey, x.buildValue)
        case _ => None
      })
      .groupByKeyAndWindow(windowLength, slideInterval)
      .flatMap((x) => {
        val uniques = x._2.toSet
        uniques.map((y) => (x._1, y))
      })
      .saveToCassandra(keyspace, tableName, SomeColumns("key", "value"))
  }
}

object TimeMatchBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new TimeMatchBenchmark(
    argHelper.CliArgs.cassandraKeyspace,
    argHelper.CliArgs.cassandraTable,
    argHelper.CliArgs.cassandraHosts,
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
