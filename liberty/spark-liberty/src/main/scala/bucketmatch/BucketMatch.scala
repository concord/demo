package com.concord.bucketmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{LogParser, SparkArgHelper, SimpleDateParser}
import org.apache.spark.streaming.{Duration, Seconds}

import org.apache.spark.streaming.dstream._

class BucketMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  override val brokers: String,
  override val topics: Set[String]
)
    extends BenchmarkStreamContext {
  private val windowLength: Int = 100
  private val slideInterval: Int = 10000

  // override def confParams: List[(String, String)] =
  //   List(("spark.cassandra.connection.host", cassandraHosts))
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 1000
  override def applicationName: String = "BucketMatch"

  override def streamLogic: Unit = {
    import com.concord.utils.EnrichedStreams._

    stream
      .countingWindow(100, 100)
      .foreachRDD(rdd => {
        println(s"There are ${rdd.count} records")
      })

    // .flatMap(x => SimpleDateParser.parse(x._2) match {
      //   case Some(x) => Some((s"${x.month}-${x.year}", x.msg))
      //   case _ => None
      // })
    //windowLength, slideInterval)
      //.map((x) => (x._1, x._2.toSet.size))
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
