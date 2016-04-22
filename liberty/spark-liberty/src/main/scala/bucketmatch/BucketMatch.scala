package com.concord.bucketmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{ LogParser, SparkArgHelper, SimpleDateParser }
import org.apache.spark.streaming.{ Duration, Seconds }

import org.apache.spark.streaming.dstream._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

class BucketMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  override val brokers: String,
  override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  private val windowLength: Int = 100000
  private val slideInterval: Int = 100000

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750 // Hasn't been calculated
  override def applicationName: String = "BucketMatch"

  override def streamLogic: Unit = {
    import com.concord.utils.EnrichedStreams._

    stream
      /** Strip bad logs, return tuple of newly built (K, V) */
      .flatMap(x => LogParser.parse(x._2) match {
        case Some(x) if (x.msg.contains("IRQ")) => Some(x.buildKey, x.buildValue)
        case _ => None
      })
      /** Break stream into discrete chunks */
      .countingWindow(windowLength, slideInterval)
      /** Erase duplicates and transform into (Key, Value) tuples */
      .flatMap(x => {
        val uniques = x._2.toSet
        uniques.map((data) => (x._1, data))
      })
      /** INSERT keyspace.tableName -- col name 'key' and 'value' and of type text */
      .saveToCassandra(keyspace, tableName, SomeColumns("key", "value"))
  }
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
