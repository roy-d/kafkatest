package lab

import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.clients.producer._

class Producer(bootStrap: String) extends Logging {
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, "0")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
  props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  def send(topic: String, message: String) =
    producer.send(
      new ProducerRecord[String, String](topic, message, message),
      new Callback() {
        override def onCompletion(metadata: RecordMetadata, e: Exception) =
          if (e != null) logger.error(e) else logger.debug("Offset of message sent: " + metadata.offset())
      }
    )

  def close() = producer.close()
}
