package dedup

import com.concord.contexts.BenchmarkStreamContext
import com.concord.utils.{LogParser, SparkArgHelper}
import org.apache.spark.streaming.{Duration, Seconds}
import kafka.producer.KeyedMessage

class Deduplication(
  override val brokers: String,
  override val topics: Set[String],
  val deduplicationMode: String) extends BenchmarkStreamContext {

  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "Deduplication"
  def streamLogicFullMsg: Unit = {
    val data = stream map { line => (LogParser.parse(line._2), 1) }
    val uniques = data.reduceByKey(_ + _)
    val tsUpFront = uniques map { line => (line._1.get.timestamp, line._1.get.buildValue) }
    // //Maybe not needed depending on equals & hashcode of LogLine?
    val sorted = tsUpFront.transform(rdd => rdd.sortByKey(true))
    sorted.print()
  }

  def streamPayloadOnly: Unit = {
    val data = stream.flatMap { line =>
      LogParser.parse(line._2)
    }
    //.map { logparser =>  logparser.msg
    // (logparser.msg, logparser)
    // TODO(asher) - this doesn't compile
    // val uniques = data.reduceByKey((m: String, l: LogParser) => m)
    // val tsUpFront = uniques map { line => (line._1.get.timestamp, line._1.get.buildValue) }
    // // //Maybe not needed depending on equals & hashcode of LogLine?
    // val sorted = tsUpFront.transform(rdd => rdd.sortByKey(true))
    // sorted.print()
  }

  override def streamLogic: Unit = {
    if(deduplicationMode.equalsIgnoreCase("entire_msg"))
        streamLogicFullMsg
    else
        streamPayloadOnly
  }
}




  object Deduplication extends App {
  val argHelper = new SparkArgHelper(args)
  new Deduplication(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet, argHelper.CliArgs.deduplicationMode
  )
}
