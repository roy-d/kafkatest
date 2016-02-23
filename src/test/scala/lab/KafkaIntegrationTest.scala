package lab

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

class KafkaIntegrationTest extends WordSpecLike with MustMatchers  with KafkaSupport with Eventually  {
  val testTopic = "it-test-topic"
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(5, Millis)))
   "Admin" must {
    "create a topic" in {
      val admin = new Admin(EmbeddedZK.zkConnect)

      admin.topicExists(testTopic) must ===(false)
      admin.createTopic(testTopic)
      admin.topicExists(testTopic) must ===(true)

      admin.close
    }
  }

  "Consumer and Producer" must {
    "can talk to each other" in {
      val bootStrap = s"127.0.0.1:$boundPort"
      val testMessage = "message in a bottle"

      val producer = new Producer(bootStrap)
      producer.send(testTopic, testMessage)
      producer.close

      val consumer = new Consumer(bootStrap, testTopic)
      eventually {
        val messages = consumer.consume(100)
        messages.size must ===(1)
        messages.head must ===(testMessage)
      }
      consumer.close()
    }
  }
}
