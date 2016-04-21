package com.concord.dedup

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.LogParser
import org.apache.spark.streaming.{Duration, Seconds}
import org.apache.spark.broadcast.Broadcast
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


class KafkaSink(createProducer: () => KafkaProducer[String, String])
    extends Serializable {
  lazy val producer = createProducer()
  def send(topic: String, key: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, key, value))
}

object KafkaSink {
  def apply(brokers: String): KafkaSink = {
    val f = () => {
      val props = new java.util.Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      println("Creating new kafka producer")
      val producer = new KafkaProducer[String, String](props)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}

class ApproxDeduplication(
  override val brokers: String,
  override val topics: Set[String],
  val outputTopic: String)
    extends BenchmarkStreamContext {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 15000
  override def applicationName: String = "ApproxDeduplication"

  override def streamLogic: Unit = {
    val kProbFalsePositive = 0.08
    val kPopulation = 265569231
    val kSeed = 1
    val bfMonoid = BloomFilter(kPopulation, kProbFalsePositive, kSeed)

    var bf = bfMonoid.zero

    val kafkaOut = stream.flatMap { line =>
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
    }
    val broadcast: Broadcast[KafkaSink] =
      kafkaOut.context.sparkContext.broadcast(KafkaSink(brokers))
    kafkaOut.foreachRDD{ rdd => (
      rdd.foreachPartition(partition => {
        partition.foreach { case (k,v) =>
          broadcast.value.send(outputTopic,k,v)
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
