package com.concord.dedup

import com.concord.contexts.{
  BenchmarkStreamContext,KafkaProducerConfiguration}
import com.concord.utils.LogParser
import org.apache.spark.streaming.{Duration, Seconds}
/**
  *  Algebird is needed w/ wild card for, serialization,
  *  bloom filters and imlicit conversions.
  */
import com.twitter.algebird._
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}

class ApproxDeduplication(
  override val brokers: String,
  override val topics: Set[String],
  val outputTopic: String)
    extends BenchmarkStreamContext with KafkaProducerConfiguration {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 15000
  override def applicationName: String = "ApproxDeduplication"
  override def streamLogic: Unit = {
    val kProbFalsePositive = 0.08
    val kPopulation = 265569231
    val kSeed = 1
    val bfMonoid = BloomFilter(kPopulation, kProbFalsePositive, kSeed)

    var bf = bfMonoid.zero

    stream.flatMap { line =>
      LogParser.parse(line._2) match {
        case Some(x) =>
          if(!bf.contains(x.msg).isTrue){
            bf = bf ++ bfMonoid.create(x.msg)
            Some((x.buildKey.toString, x.buildValue))
          }else {
            None
          }
        case None => None
      }
    }.foreachRDD{ rdd => (
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String,String](producerProps)
        partition.foreach { case (k,v) =>
          val r = new ProducerRecord[String,String](outputTopic,k,v)
          producer.send(r)
        }
      })
    )}
  }
}

class ApproxArgs(args: Array[String]) {
  object cli {
    @Option(name = "-kafka_brokers", usage = "i.e. localhost:9092,1.1.1.2:9092")
    var kafkaBrokers: String = ""
    @Option(name = "-output_topic", usage = "kafka topics separated by ,")
    var outputTopic: String = "approx_uniq"
    @Option(name = "-input_topic", usage = "kafka topics separated by ,")
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
