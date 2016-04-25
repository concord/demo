package com.concord.dedup

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}
object KafkaSink {
  @transient private var producer: KafkaProducer[String, String] = null;
  def getInstance(brokers: String): KafkaProducer[String, String] = {
    if (producer == null) {
      synchronized {
        val props = new java.util.Properties()
        props.put("bootstrap.servers", brokers)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer[String, String](props)
        sys.addShutdownHook {
          producer.close
        }
      }
    }
    producer
  }
}
