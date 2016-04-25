package com.concord.dedup
import com.concord.utils.LogParser
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Duration, Seconds }
import org.apache.spark.broadcast.Broadcast
/**
 *  Algebird is needed w/ wild card for, serialization,
 *  bloom filters and imlicit conversions.
 */
import com.twitter.algebird._
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option => CLIOption }
import org.apache.kafka.clients.producer.ProducerRecord
import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream

class ApproxDeduplication(
    val brokers: String,
    val topics: Set[String],
    val outputTopic: String
) extends Serializable {
  def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .setAppName(applicationName)
      .set("spark.mesos.executor.home", "/usr/lib/spark")
      .set("spark.default.parallelism", "120")
      .set("spark.streaming.kafka.maxRatePerPartition", streamingRate.toString)
    conf
  }
  private val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "auto.offset.reset" -> "smallest"
  )

  def batchInterval: Duration = Seconds(1)
  def streamingRate: Int = 10
  def applicationName: String = "ApproxDeduplication"
  def start(): Unit = {
    val sparkContext = new SparkContext(defaultSparkConf)
    val streamingSparkContext = new StreamingContext(sparkContext, batchInterval)
    val stream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
        streamingSparkContext, kafkaParams, topics
      )

    val kProbFalsePositive = 0.08
    val kPopulation = 265569231
    val kSeed = 1
    val bfMonoid = BloomFilter(kPopulation, kProbFalsePositive, kSeed)
    var bf = bfMonoid.zero

    val kafkaOut = stream.flatMap { line =>
      LogParser.parse(line._2) match {
        case Some(x) =>
          val (newBF, contained) = bf.checkAndAdd(x.msg)
          if (contained.isTrue) {
            None
          } else {
            bf = newBF
            Some((x.buildKey.toString, x.buildValue))
          }
        case None => None
      }
    }

    kafkaOut.foreachRDD(rdd => {
      // the set of rdd's w/out a bloomfilter which never finishes
      //
      rdd.foreachPartition(part => {
        val producer = KafkaSink.getInstance(brokers)
        part.foreach(record =>
          producer.send(new ProducerRecord(outputTopic, record._1, record._2)))
      })
    })
    streamingSparkContext.start()
    streamingSparkContext.awaitTermination()
  }
}

object ApproxDeduplication extends App {
  class ApproxArgs(args: Array[String]) {
    object cli {
      @CLIOption(name = "-kafka_brokers", usage = "i.e. localhost:9092,1.1.1.2:9092")
      var kafkaBrokers: String = ""
      @CLIOption(name = "-output_topic", usage = "kafka topics separated by ,")
      var outputTopic: String = "approx_uniq"
      @CLIOption(name = "-input_topic", usage = "kafka topics separated by ,")
      var kafkaTopics: String = "liberty"
    }
    val parser = new CmdLineParser(cli)
    try {
      import scala.collection.JavaConversions._
      parser.parseArgument(args.toList)
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }
  }
  val argv = new ApproxArgs(args)
  println(s"Arguments: ${argv.cli}")
  new ApproxDeduplication(
    argv.cli.kafkaBrokers,
    argv.cli.kafkaTopics.split(",").toSet,
    argv.cli.outputTopic
  ).start
}
