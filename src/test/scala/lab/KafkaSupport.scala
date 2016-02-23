package lab

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, SystemTime}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait KafkaSupport extends BeforeAndAfterAll with Logging{
  this: Suite =>

  private var serverRef: Option[KafkaServer] = None

  def boundPort: Int = serverRef.getOrElse(fail("kafka server not started")).boundPort()

  override def beforeAll(): Unit = {
    val props = new Properties
    props.put("broker.id","0")
    props.put("log.dir", EmbeddedZK.tempDir().getAbsolutePath)
    props.put("zookeeper.connect", EmbeddedZK.zkConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("controller.socket.timeout.ms", "1500")
    props.put("controlled.shutdown.retry.backoff.ms", "100")
    props.put("log.cleaner.dedupe.buffer.size", "2097152")

    val server = new KafkaServer(KafkaConfig.fromProps(props), SystemTime)
    server.startup()
    serverRef = Some(server)
    logger.debug("Kafka Broker started")
  }

  override def afterAll(): Unit = {
    serverRef.get.shutdown()
    serverRef.get.awaitShutdown()
    logger.debug("Kafka Broker shut down")
  }

}
