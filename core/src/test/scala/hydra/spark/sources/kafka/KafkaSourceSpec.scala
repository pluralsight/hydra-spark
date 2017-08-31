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

import hydra.spark.api.{Invalid, Valid}
import hydra.spark.testutils.{KafkaTestSupport, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by alexsilva on 6/2/16.
  */
class KafkaSourceSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterEach with SharedSparkContext with KafkaTestSupport
  with BeforeAndAfterAll {

  val topics: Map[String, Map[String, Any]] = Map(
    "testJson" -> jsonProps,
    "testAvro" -> avroProps
  )


  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(1, Seconds))

  var sctx: StreamingContext = _

  override def afterEach(): Unit = {
    super.afterEach()
    Option(sctx).foreach(_.stop(false, false))
  }

  override def beforeEach() = {
    super.beforeEach()
  }

  describe("The Kafka Source") {
    it("Should be valid with all the basic properties") {
      val properties = Map("bootstrap.servers" -> "localhost:6001")
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
    val properties = Map(
      "bootstrap.servers" -> "localhost:6001",
      "zookeeper.connect" -> "localhost:6000",
      "group.id" -> "json-stream-test")

    val kafkaMessages = publishJson("json-stream")
    val source = KafkaSource(Map("json-stream" -> jsonProps), properties)

    sctx = new StreamingContext(sc, org.apache.spark.streaming.Seconds(1))

    val stream = source.createStream(sctx)
    val msgs = new ArrayBuffer[String]()
    stream.foreachRDD {
      msgs ++= _.map(_.value.toString).collect()
    }

    sctx.start()
    eventually {
      msgs shouldEqual kafkaMessages
    }
  }

  it("should create the RDD") {
    import spray.json._
    val properties = Map(
      "bootstrap.servers" -> "localhost:6001",
      "zookeeper.connect" -> "localhost:6000",
      "group.id" -> "test-rdd")
    val kafkaMessages = publishJson("test-rdd")
    val source = KafkaSource(Map("test-rdd" -> jsonProps), properties)
    val df = source.createDF(ss)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val msgs = df.toJSON.map(_.toString).collect().map(_.parseJson)

    eventually {
      msgs should contain theSameElementsAs kafkaMessages.map(_.parseJson)
    }

  }

  it("includes the key in the json payload when specified") {
    import spray.json._
    val properties = Map(
      "bootstrap.servers" -> "localhost:6001",
      "zookeeper.connect" -> "localhost:6000",
      "group.id" -> "test-json-key")
    val topicProps = jsonProps + ("keyColumn" -> "messageKey")
    val kafkaMessages = publishJsonWithKeys("test-json-key")
    val source = KafkaSource(Map("test-json-key" -> topicProps), properties)
    sctx = new StreamingContext(sc, org.apache.spark.streaming.Seconds(1))
    val stream = source.createStream(sctx)

    val msgs = new ArrayBuffer[JsValue]()
    stream.foreachRDD {
      msgs ++= _.map(_.value.toString).collect().map(_.parseJson)
    }

    sctx.start()

    val keyedMessages: Seq[JsValue] = kafkaMessages.map { json =>
      val pj = json.parseJson.asJsObject
      JsObject(pj.fields + ("messageKey" -> JsString(pj.fields("messageId").compactPrint)))
    }

    eventually {
      msgs shouldEqual keyedMessages
    }

  }

  ignore("includes the key in the avro payload when specified") {
    import spray.json._
    val properties = Map(
      "bootstrap.servers" -> "localhost:6001",
      "zookeeper.connect" -> "localhost:6000",
      "group.id" -> "hydra-avro-keys",
    "schema.registry.url" -> "http://localhost:8081"
    )

    val records = publishAvroWithKeys("test-avro-keys")

    val topicProps = avroProps + ("keyColumn" -> "messageKey",
      "schema" -> "test-schema")

    val source = KafkaSource(Map("test-avro-keys" -> topicProps), properties)
    sctx = new StreamingContext(sc, org.apache.spark.streaming.Seconds(1))
    val stream = source.createStream(sctx)
    val msgs = new ArrayBuffer[String]()

    stream.foreachRDD { rdd =>
      msgs ++= rdd.map(_.value.toString).collect()
    }


    sctx.start()

    val keyedMessages = records.map { record =>
      val pj = record.toString.parseJson.asJsObject
      JsObject(pj.fields + ("messageKey" -> pj.fields("messageId"))).toString()
    }

    eventually {
      msgs shouldEqual keyedMessages
    }
  }
}