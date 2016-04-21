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
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}
import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream

object KafkaSink {
  def newProducer(brokers: String): KafkaProducer[String, String] = {
    val props = new java.util.Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    new KafkaProducer[String, String](props)
  }
}

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
  def streamingRate: Int = 8000
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
    stream.flatMap { line =>
      LogParser.parse(line._2) match {
        case Some(x) =>
          if (!bf.contains(x.msg).isTrue) {
            bf = bf ++ bfMonoid.create(x.msg)
            Some((x.buildKey.toString, x.buildValue))
          } else {
            None
          }
        case None => None
      }
    }.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        val producer = KafkaSink.newProducer(brokers)
        part.foreach(record =>
          producer.send(new ProducerRecord(outputTopic, record._1, record._2)))
        producer.close
      })
    })
    streamingSparkContext.start()
    streamingSparkContext.awaitTermination()
  }
}

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

object ApproxDeduplication extends App {
  val argv = new ApproxArgs(args)
  new ApproxDeduplication(
    argv.cli.kafkaBrokers,
    argv.cli.kafkaTopics.split(",").toSet,
    argv.cli.outputTopic
  ).start
}
