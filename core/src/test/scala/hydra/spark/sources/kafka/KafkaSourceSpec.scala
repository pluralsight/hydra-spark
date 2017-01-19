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

package hydra.spark.sources.kafka

import hydra.spark.api.{ Invalid, Valid }
import hydra.spark.testutils.{ SharedKafka, SharedSparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration, ScalaFutures }
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers }

import scala.collection.mutable.ArrayBuffer

/**
 * Created by alexsilva on 6/2/16.
 */
class KafkaSourceSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
    with Eventually with BeforeAndAfterAll with BeforeAndAfterEach with SharedKafka with SharedSparkContext {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(1, Seconds))

  var sctx: StreamingContext = _

  override def afterEach(): Unit = {
    super.afterEach()
    sctx.stop(false, true)
  }

  override def beforeEach() = {
    super.beforeEach()
    MsgHolder.msgs.clear()
    sctx = new StreamingContext(sc, org.apache.spark.streaming.Seconds(1))
  }

  describe("The Kafka Source") {
    it("Should be valid with all the basic properties") {
      val properties = Map("metadata.broker.list" -> "localhost:5001")
      val source = KafkaSource(topics, properties)
      source.validate() shouldBe Valid
    }

  }
  it("Should be invalid if an important property is missing") {
    val props = Map.empty[String, String]
    val source = KafkaSource(topics, props)
    val validation = source.validate().asInstanceOf[Invalid]
    validation.errors shouldBe Invalid.unapply(validation).get
  }

  it("Should Create the json stream") {
    val properties = Map("metadata.broker.list" -> "localhost:5001", "group.id" -> "test")
    val source = KafkaSource(Map("testJson" -> jsonTopic), properties)

    val stream = source.createStream(sctx)

    stream.foreachRDD { rdd =>
      rdd.foreach(msg => MsgHolder.msgs += msg.value.toString)
    }

    sctx.start()
    eventually {
      MsgHolder.msgs shouldEqual jsonMessages
    }
  }

  it("should create the RDD") {
    val properties = Map("metadata.broker.list" -> "localhost:5001", "group.id" -> "test")
    val source = KafkaSource(Map("testJson" -> jsonTopic), properties)
    source.createDF(new SQLContext(sc)).count() shouldBe 11
  }

  it("includes the key in the json payload when specified") {
    import spray.json._
    val properties = Map("metadata.broker.list" -> "localhost:5001", "group.id" -> "test")
    val topicProps = jsonTopic + ("keyColumn" -> "key")
    val source = KafkaSource(Map("testJson" -> topicProps), properties)
    val stream = source.createStream(sctx)

    stream.foreachRDD { rdd =>
      rdd.foreach(msg => MsgHolder.msgs += msg.value.toString)
    }

    sctx.start()

    val keyedMessages = jsonMessages.map { json =>
      val pj = json.parseJson.asJsObject
      JsObject(pj.fields + ("key" -> pj.fields("no"))).toString()
    }

    eventually {
      MsgHolder.msgs shouldEqual keyedMessages
    }
  }

  it("Checkpoints batch jobs") {
    //KAFKA UNIT DOES NOT SUPPORT TOPIC COORDINATORS; HOW CAN WE UNIT TEST THIS???

    //    val properties = Map("metadata.broker.list" -> "localhost:5001", "group.id" -> "test-checkpoint")
    //    val source = KafkaSource(Map("testJson" -> jsonTopic), properties)
    //    source.createDF(new SQLContext(ctx.get)).count() shouldBe 11
    //    val groupSource = KafkaSource(Map("testJson" ->  (jsonTopic + ("start" -> "last"))), properties)
    //    groupSource.createDF(new SQLContext(ctx.get)).count() shouldBe 0
    //    //we then publish messages
    //    kafka.sendMessages(new KeyedMessage("testJson","test","test"))
    //
    //    val groupSource2 = KafkaSource(Map("testJson" ->  (jsonTopic + ("start" -> "last"))), properties)
    //    groupSource2.createDF(new SQLContext(ctx.get)).count() shouldBe 1
  }

  it("includes the key in the avro payload when specified") {
    import spray.json._
    val properties = Map(
      "metadata.broker.list" -> "localhost:5001",
      "group.id" -> "test",
      "schema.registry.url" -> "localhost:8081"
    )
    val topicProps = avroTopic + ("keyColumn" -> "key")
    val source = KafkaSource(Map("testAvro" -> topicProps), properties)
    val stream = source.createStream(sctx)

    stream.foreachRDD { rdd =>
      rdd.foreach(msg => MsgHolder.msgs += msg.value.toString)
    }

    sctx.start()

    val keyedMessages = jsonMessages.map { json =>
      val pj = json.parseJson.asJsObject
      JsObject(pj.fields + ("key" -> pj.fields("no"))).toString()
    }

    eventually {
      MsgHolder.msgs shouldEqual keyedMessages
    }
  }
}

object MsgHolder {
  val msgs = new ArrayBuffer[String]()
}