package com.concord.pmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/** Implement a computation that consumes a kafka topics and publishes a unique
  * key to Cassandra for every item that contains the string 'IRQ'.
  */
class PatternMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  brokers: String,
  topics: Set[String])
    extends BenchmarkStreamContext(brokers, topics) {
  /** Add additional cassandra param field to superclass configurations params */
  sparkConf.set("spark.cassandra.connection.host", cassandraHosts)

  /** Overidden constants and impl */
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 1000
  override def applicationName: String = "PatternMatch"

  /** Custom logic transforms superclasses kafka istream */
  override def streamLogic: Unit = {
    /** Build DStream[(k, v)] of valid logs with specially
      * constructed key. Documentation of key is in utils.LogParser
      * After filtering non matches and improper logs push to cassandra*/
    stream
      .map(x => LogParser.parse(x._2) match {
        case Some(x) if (x.msg.contains("IRQ")) => Some(x.buildKey, x.buildValue)
        case _ => None
      })
      .filter(!_.isEmpty)
      .map(_.get)
      .saveToCassandra(keyspace, tableName, SomeColumns("key", "value"))
  }
}

object PatternMatchBenchmark {
  def main(args: Array[String]): Unit = {
    val argHelper = new SparkArgHelper(args)
    new PatternMatchBenchmark(
      argHelper.CliArgs.cassandraKeyspace,
      argHelper.CliArgs.cassandraTable,
      argHelper.CliArgs.cassandraHosts,
      argHelper.CliArgs.kafkaBrokers,
      argHelper.CliArgs.kafkaTopics.split(",").toSet
    )
  }
}
