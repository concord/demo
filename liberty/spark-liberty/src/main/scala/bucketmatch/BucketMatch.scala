package com.concord.bucketmatch

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import com.concord.utils.SparkArgHelper

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import scala.collection.mutable.{Queue => MutableQueue}
import scala.collection.mutable.MutableList

sealed case class Window(
  val id: Int,
  val records: MutableList[(String, String)]) {}

class BucketMatchBenchmark(
  keyspace: String,
  tableName: String,
  cassandraHosts: String,
  override val brokers: String,
  override val topics: Set[String]) extends BenchmarkStreamContext {
  /** Immutable constants */
  private val windowLength: Int = 100000
  private val slideInterval: Int = 100000

  /** 'Global' serializable variables */
  private var recordsProcessed: Int = 0
  private var windowCount: Int = 0

  override def confParams: List[(String, String)] =
    List(("spark.cassandra.connection.host", cassandraHosts))
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = ???
  override def applicationName: String = "BucketMatch"

  override def streamLogic: Unit = {
    val windows: MutableQueue[Window] = MutableQueue()
    stream.foreachRDD(rdd => {
      val elements = rdd.collect()
      elements.foreach( e => {
        /** Process and remove full windows */
        windows
          .dequeueAll(w => w.records.size >= windowLength)
          .foreach(processWindow)

        /** Create new window if count is within slide */
        if(isNextWindowReady) {
          windows += Window(windowCount, List())
          windowCount += 1
        }

        /** Push all records into open windows */
        windows.map(w => w.records += element)

        /** Increment record count */
        recordsProcessed += 1
      })
    })
  }

  def isNextWindowReady: Boolean =
    if (recordsProcessed == 0)
      true
    else
      recordsProcessed % slideInterval == 0

  def processWindow(window: Window): Unit = {

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
