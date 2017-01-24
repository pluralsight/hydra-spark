package hydra.spark.testutils

import com.google.common.collect.ArrayListMultimap
import info.batey.kafka.unit.KafkaUnit
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException}
import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import org.junit.ComparisonFailure

/**
  * Created by alexsilva on 1/24/17.
  */
class KafkaUnitWithKeys(zkPort: Int, brokerPort: Int) extends KafkaUnit(zkPort, brokerPort) {

  def readMessagesWithKey(topicName: String, expectedMessages: Int): ArrayListMultimap[String, String] = {
    import java.util._
    import java.util.concurrent.{Executors, TimeoutException, _}
    val singleThread = Executors.newSingleThreadExecutor
    val consumerProperties = new java.util.Properties
    consumerProperties.put("zookeeper.connect", "localhost:5000")
    consumerProperties.put("group.id", "10")
    consumerProperties.put("socket.timeout.ms", "500")
    consumerProperties.put("consumer.id", "test")
    consumerProperties.put("auto.offset.reset", "smallest")
    consumerProperties.put("consumer.timeout.ms", "500")
    val javaConsumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties))
    val stringDecoder = new StringDecoder(new VerifiableProperties(new Properties))
    val topicMap = new java.util.HashMap[String, Integer]()
    topicMap.put(topicName, 1)
    val events = javaConsumerConnector.createMessageStreams(topicMap, stringDecoder, stringDecoder)
    val events1 = events.get(topicName)
    val kafkaStreams = events1.get(0)
    val submit = singleThread.submit(new Callable[ArrayListMultimap[String, String]]() {
      @throws[Exception]
      def call: ArrayListMultimap[String, String] = {
        val messages = ArrayListMultimap.create[String, String]()
        try
            for (kafkaStream <- kafkaStreams) {
              val message = kafkaStream.message
              messages.put(kafkaStream.key(), message)
            }

        catch {
          case e: ConsumerTimeoutException => {
            // always gets throws reaching the end of the stream
          }
        }
        if (messages.size != expectedMessages) throw new ComparisonFailure("Incorrect number of messages returned", Integer.toString(expectedMessages), Integer.toString(messages.size))
        messages
      }
    })
    try
      submit.get(3, TimeUnit.SECONDS)

    catch {
      case e: Any => {
        if (e.getCause.isInstanceOf[ComparisonFailure]) throw e.getCause.asInstanceOf[ComparisonFailure]
        throw new TimeoutException("Timed out waiting for messages")
      }
    } finally {
      singleThread.shutdown()
      javaConsumerConnector.shutdown()
    }
  }

}
