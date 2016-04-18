package concord.demo.liberty.spark

import io.concord.eval.LineParser

import com.concord.utils.SparkArgHelper

import kafka.serializer.{ DefaultDecoder, StringDecoder }

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.kafka._

import scala.collection.JavaConversions._

/**
 * Created by adev on 4/4/16.
 */
object Deduplication extends App {

  val argHelper = new SparkArgHelper(args)

  val conf = new SparkConf().setAppName("Deduplicator").setMaster(argHelper.CliArgs.masterUrl)
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
  conf.set("spark.shuffle.manager", "SORT")
  conf.set("spark.streaming.unpersist", "true")

  val ssc = new StreamingContext(conf, Seconds(1))
  // SET KAFKA SPECIFIC CONFIG SETTINGS
  val kafkaConfigSettings = Map[String, String](
    "metadata.broker.list" -> argHelper.CliArgs.kafkaBrokers)

  val kafkaTopics: Set[String] = argHelper.CliArgs.kafkaTopics.split(",").toSet

  // SETUP THE STREAM
  val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
    ssc, kafkaConfigSettings, kafkaTopics).map(_._2)

  val data = lines map { line => (LineParser(line), 1) }
  val uniques = data.reduceByKey(_ + _)
  val tsUpFront = uniques map { line => (line._1.get.tsSecs, line._1.get.fullLine) }
  //Maybe not needed depending on equals & hashcode of LogLine?
  val sorted = tsUpFront.transform(rdd => rdd.sortByKey(true))

  // OUTPUT TO KAFKA HERE...

  // Run the streaming job
  //ssc.start()
  //ssc.awaitTermination()
}
