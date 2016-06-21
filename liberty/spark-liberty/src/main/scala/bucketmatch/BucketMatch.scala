package com.concord.bucketmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{ LogParser, SparkArgHelper, SimpleDateParser }
import org.apache.spark.streaming.{ Duration, Seconds }

import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.RDD

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

import scala.collection.mutable.{ Queue => MutableQueue }

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

  override def confParams: List[(String, String)] =
    List(("spark.cassandra.connection.host", cassandraHosts))

  override def batchInterval: Duration = Seconds(7)
  override def streamingRate: Int = 750
  override def applicationName: String = "BucketMatch"

  override def streamLogic: Unit = {
    import com.concord.utils.EnrichedStreams._

    implicit val queue = MutableQueue[RDD[(Int, String)]]()

    stream
      /** Strip bad logs, return tuple of newly built (K, V) */
      .flatMap(x => LogParser.parse(x._2) match {
        case Some(x) if (x.msg.contains("IRQ")) => Some(x.buildKey, x.buildValue)
        case _ => None
      })
      /** Break stream into discrete chunks */
      .countingWindow(windowLength, slideInterval)
      /** Erase duplicates and transform into (Key, Value) tuples */
      .flatMap(_.toSet)
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
