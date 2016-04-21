package com.concord.contexts
import java.util.Properties
trait KafkaProducerConfiguration {
  def brokers: String = ???
  def producerProps: Properties = {
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", brokers)
    producerConf.put("request.required.acks", "1")
    producerConf
  }
}
