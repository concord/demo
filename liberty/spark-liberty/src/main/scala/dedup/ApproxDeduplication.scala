package com.concord.dedup

import com.concord.contexts.{
  BenchmarkStreamContext,KafkaProducerConfiguration}
import com.concord.utils.LogParser
import org.apache.spark.streaming.{Duration, Seconds}
/**
  * We need kafkawriter's implicit type to turn a DStream[T]
  * into a stream that can save into kafka.
  */
import org.cloudera.spark.streaming.kafka.KafkaWriter._
import kafka.producer.KeyedMessage
/**
  *  Algebird is needed w/ wild card for, serialization,
  *  bloom filters and imlicit conversions.
  */
import com.twitter.algebird._
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}


class ApproxDeduplication(
  override val brokers: String,
  override val topics: Set[String],
  val outputTopic: String)
    extends BenchmarkStreamContext with KafkaProducerConfiguration {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "ApproxDeduplication"
  override def streamLogic: Unit = {
    val NUM_HASHES = 6
    val WIDTH = 32
    val SEED = 1
    val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)

    var bf = bfMonoid.zero

    val data = stream.flatMap { line =>
      LogParser.parse(line._2) match {
        case Some(x) =>
          if(!bf.contains(x.msg).isTrue){
            bf = bfMonoid.create(x.msg)
            Some((x.buildKey.toString, x.buildValue))
          }else {
            None
          }
        case None => None
      }
    }.writeToKafka(producerProps, (x) => {
      new KeyedMessage[String,String](outputTopic,x._1,x._2)
    })
  }
}

class ApproxArgs(args: Array[String]) {
  import scala.collection.JavaConversions._
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
