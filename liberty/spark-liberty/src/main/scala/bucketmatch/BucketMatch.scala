package com.concord.bucketmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration

class BucketMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  override val brokers: String,
  override val topics: Set[String]) extends BenchmarkStreamContext {
  /** Immutable constants */
  private val windowLength: Int = 100000
  private val slideInterval: Int = 100000

  override def confParams: List[(String, String)] =
    List(("spark.cassandra.connection.host", cassandraHosts))
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = ???
  override def applicationName: String = "BucketMatch"

  override def streamLogic: Unit = {}
}

object BucketMatchBenchmark extends App {
  val argHelper = new SparkArgHelper(args)
  new BucketMatchBenchmark(
    argHelper.CliArgs.cassandraKeyspace,
    argHelper.CliArgs.cassandraTable,
    argHelper.CliArgs.cassandraHosts,
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  ).start
}
