/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.operations.io


import java.util.Properties

import com.typesafe.config.ConfigFactory
import hydra.spark.api.{Invalid, Valid}
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import hydra.spark.transform.SparkBatchTransformation
import kafka.serializer.StringEncoder
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, KafkaUnavailableException}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.ListBuffer
import scala.concurrent.TimeoutException
import scala.util.Try

/**
  * Created by alexsilva on 8/9/16.
  */
class PublishToKafkaSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterAll with Eventually
  with BeforeAndAfterEach with SharedSparkContext with EmbeddedKafka {

  val props = ConfigFactory.parseString(
    s"""
       |spark.master = "local[1]"
       |spark.default.parallelism	= 1
       |spark.ui.enabled = false
       |spark.driver.allowMultipleContexts = false
        """.stripMargin
  )

  val kafkaProps = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:6001",
    "producer.type" -> "sync",
    "batch.size" -> "1",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName(),
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName(),
    "serializer.class" -> classOf[StringEncoder].getCanonicalName()
  )

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  describe("When writing to Kafka") {
    it("Should be invalid without a topic") {
      val op = PublishToKafka("", properties = Map.empty)
      val validation = op.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get

      val op1 = PublishToKafka("test", properties = Map.empty)
      val validation1 = op1.validate.asInstanceOf[Invalid]
      validation1.errors shouldBe Invalid.unapply(validation1).get
    }

    it("Should produce unordered messages") {

      val op = PublishToKafka("test-und-topic", properties = kafkaProps)

      val sd = SparkBatchTransformation("test", StaticJsonSource, Seq(op), props)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-und-topic", StaticJsonSource.msgs.size)

      eventually {
        msgs.size shouldBe StaticJsonSource.msgs.size
      }

    }

    it("Should produce ordered messages") {
      import spray.json._

      val op = PublishToKafka("test-ord-topic", orderBy = Some("msg_no desc"), properties = kafkaProps)

      val sd = SparkBatchTransformation("test", StaticJsonSource, Seq(op), props)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-ord-topic", StaticJsonSource.msgs.size)

      val sorted = StaticJsonSource.msgs.sortWith(_.parseJson.asJsObject.fields("msg_no").asInstanceOf[JsNumber]
        .value > _.parseJson.asJsObject.fields("msg_no").asInstanceOf[JsNumber].value)

      for (i <- 0 until msgs.size)
        msgs(i).parseJson.asJsObject.fields("msg_no").asInstanceOf[JsNumber].value shouldBe
          sorted(i).parseJson.asJsObject.fields("msg_no").asInstanceOf[JsNumber].value

    }

    it("Should only select the columns specified") {

      import spray.json._

      val op = PublishToKafka("test-sel-topic", columns = Some(List("msg_no")), properties = kafkaProps)

      val sd = SparkBatchTransformation("test", StaticJsonSource, Seq(op), props)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-sel-topic", StaticJsonSource.msgs.size)

      msgs.size shouldBe StaticJsonSource.msgs.size
      for (i <- 0 until msgs.size) {
        intercept[NoSuchElementException] {
          msgs(i).parseJson.asJsObject.fields("data")
        }
      }
    }
  }

  it("Should produce messages with a key") {
    import spray.json._
    import DefaultJsonProtocol._

    val op = PublishToKafka("test-key-topic",
      key = Some("msg_no"), orderBy = Some("msg_no desc"),
      properties = kafkaProps)

    val sd = SparkBatchTransformation("test", StaticJsonSource, Seq(op), props)

    sd.validate shouldBe Valid

    sd.run()

    val sortedNoKeys = StaticJsonSource.msgs.map { json =>
      val fields = json.parseJson.asJsObject.fields
      fields("msg_no").convertTo[Int] -> JsObject(fields - "msg_no").fields("data")
    }.sortWith(_._1 > _._1).toList.map(r=>r._1.toString -> r._2)

    val keyed = consumeKeyedMessagesFrom("test-key-topic", 11, true)(implicitly[EmbeddedKafkaConfig],
      new StringDeserializer)

    keyed
      .map(r => r.key() -> r.value().parseJson.asJsObject.fields("data")) should contain theSameElementsAs sortedNoKeys
  }

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
  }


  def consumeKeyedMessagesFrom[V](topic: String,
                                  number: Int,
                                  autoCommit: Boolean = false)(
                                   implicit config: EmbeddedKafkaConfig,
                                   deserializer: Deserializer[V]): List[ConsumerRecord[String, V]] = {

    import scala.collection.JavaConverters._

    val props = new Properties()
    props.put("group.id", "hydra-spark")
    props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("enable.auto.commit", autoCommit.toString)

    val consumer =
      new KafkaConsumer[String, V](props, new StringDeserializer, deserializer)

    val messages = Try {
      val messagesBuffer = ListBuffer.empty[ConsumerRecord[String, V]]
      var messagesRead = 0
      consumer.subscribe(List(topic).asJava)
      consumer.partitionsFor(topic)

      while (messagesRead < number) {
        val records = consumer.poll(5000)
        if (records.isEmpty) {
          throw new TimeoutException(
            "Unable to retrieve a message from Kafka in 5000ms")
        }

        val recordIter = records.iterator()
        while (recordIter.hasNext && messagesRead < number) {
          val record = recordIter.next()
          messagesBuffer += record
          val tp = new TopicPartition(record.topic(), record.partition())
          val om = new OffsetAndMetadata(record.offset() + 1)
          consumer.commitSync(Map(tp -> om).asJava)
          messagesRead += 1
        }
      }
      messagesBuffer.toList
    }

    consumer.close()
    messages.recover {
      case ex: KafkaException => throw new KafkaUnavailableException(ex)
    }.get
  }


}