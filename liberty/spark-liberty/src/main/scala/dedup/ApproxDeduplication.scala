package dedup

import com.concord.contexts.{
  BenchmarkStreamContext,KafkaProducerConfiguration}
import com.concord.utils.{LogParser, SparkArgHelper}
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

class ApproxDeduplication(
  override val brokers: String,
  override val topics: Set[String])
    extends BenchmarkStreamContext with KafkaProducerConfiguration {
  override def batchInterval: Duration = Seconds(1)
  override def streamingRate: Int = 750
  override def applicationName: String = "ApproxDeduplication"
  override def streamLogic: Unit = {

    // bloom filter w/ entries.

    val data = stream.flatMap { line =>
      LogParser.parse(line._2)
    }.writeToKafka(producerProps, (x: LogParser) => {
      new KeyedMessage[String,String]("foobartopic",
        x.buildKey.toString, x.buildValue)
    })
}




  object ApproxDeduplication extends App {
  val argHelper = new SparkArgHelper(args)
  new ApproxDeduplication(
    argHelper.CliArgs.kafkaBrokers,
    argHelper.CliArgs.kafkaTopics.split(",").toSet
  )
}
