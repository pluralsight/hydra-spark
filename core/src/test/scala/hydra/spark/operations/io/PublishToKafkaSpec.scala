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


import com.typesafe.config.ConfigFactory
import hydra.spark.api.{Invalid, Operations, Valid}
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import kafka.serializer.StringEncoder
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest._
import org.scalatest.concurrent.Eventually
import net.manub.embeddedkafka.ConsumerExtensions._

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
    "metadata.broker.list" -> "localhost:5001",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:5001",
    "producer.type" -> "sync",
    "batch.size" -> "1",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName(),
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName(),
    "serializer.class" -> classOf[StringEncoder].getCanonicalName()
  )

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test-topic")
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

      val op = PublishToKafka("test-topic", properties = kafkaProps)

      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(op), props, ss)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-topic", StaticJsonSource.msgs.size)

      eventually {
        msgs.size shouldBe StaticJsonSource.msgs.size
      }

    }

    it("Should produce ordered messages") {
      import spray.json._

      val op = PublishToKafka("test-topic", orderBy = Some("msg-no desc"), properties = kafkaProps)

      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(op), props, ss)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-topic", StaticJsonSource.msgs.size)

      val sorted = StaticJsonSource.msgs.sortWith(_.parseJson.asJsObject.fields("msg-no").asInstanceOf[JsNumber]
        .value > _.parseJson.asJsObject.fields("msg-no").asInstanceOf[JsNumber].value)

      for (i <- 0 until msgs.size)
        msgs(i).parseJson.asJsObject.fields("msg-no").asInstanceOf[JsNumber].value shouldBe
          sorted(i).parseJson.asJsObject.fields("msg-no").asInstanceOf[JsNumber].value

    }

    it("Should only select the columns specified") {

      import spray.json._

      val op = PublishToKafka("test-topic", columns = Some(List("msg-no")), properties = kafkaProps)

      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(op), props, ss)

      sd.validate shouldBe Valid

      sd.run()

      val msgs = EmbeddedKafka.consumeNumberStringMessagesFrom("test-topic", StaticJsonSource.msgs.size)

      msgs.size shouldBe StaticJsonSource.msgs.size
      for (i <- 0 until msgs.size) {
        intercept[NoSuchElementException] {
          msgs(i).parseJson.asJsObject.fields("data")
        }
      }
    }
  }

  ignore("Should produce messages with a key") {
    import spray.json._
    import DefaultJsonProtocol._

    val op = PublishToKafka("test-topic", key = Some("msg-no"), orderBy = Some("msg-no desc"), properties = kafkaProps)

    val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(op), props, ss)

    sd.validate shouldBe Valid

    sd.run()

    val sortedNoKeys = StaticJsonSource.msgs.map { json =>
      val fields = json.parseJson.asJsObject.fields
      fields("msg-no").convertTo[Int] -> JsObject(fields - "msg-no").compactPrint
    }.sortWith(_._1 < _._1)
  }

  override def afterAll() = {
    super.afterAll()
    EmbeddedKafka.stop()
  }


}
