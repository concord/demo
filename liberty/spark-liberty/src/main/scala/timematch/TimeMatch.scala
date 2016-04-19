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
  brokers: String,
  topics: Set[String])
    extends BenchmarkStreamContext(brokers, topics) {
  /** Add additional cassandra param field to superclass configurations params */
  sparkConf.set("spark.cassandra.connection.host", cassandraHosts)

  /** Constants */
  private val windowLength: Duration = Seconds(10)
  private val slideInterval: Duration = Seconds(10)

  /** Overidden constants and impl */
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 3400 // ~ 500k / 144(partitions)
  override def applicationName: String = "TimeMatch"

  /** If we could parameterize a DStream over a guarenteed serializble type, then
    * we could extend the logic from com.concord.pmatch.PatternMatchBenchmark */
  override def streamLogic: Unit = {
    stream
      .map(x => LogParser.parse(x._2) match {
        case Some(x) if (x.msg.contains("IRQ")) => Some(x.buildKey, x.buildValue)
        case _ => None
      })
      .filter(!_.isEmpty)
      .map(_.get)
      .window(windowLength, slideInterval)
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
  )
}
