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

import java.util.Properties

import hydra.spark.api.InvalidDslException
import hydra.spark.testutils.KafkaTestSupport
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 12/12/16.
  */
class OffsetsSpec extends Matchers with FunSpecLike with BeforeAndAfterAll with Eventually with KafkaTestSupport {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  //setting the group id to 10 because KafkaUnit sets it to 10 as well within its readMessages method...
  val props = new Properties()
  props.putAll(Map(
    "bootstrap.servers" -> bootstrapServers,
    "zookeeper.connect" -> zkConnect, "group.id" -> "10"
  ).asJava)
  val cfg = new ConsumerConfig(props)

  describe("Kafka offsets") {
    it("Should translate topic start/stop values into the correct number") {
      var properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "largest")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.LatestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "smallest")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "-2")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "-1")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.LatestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "1243345")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe 1243345
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "NaN")
      intercept[InvalidDslException] {
        Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime)
      }

      properties = Map("metadata.broker.list" -> "localhost:5001", "start" -> "last")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe -3
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime
    }

    it("Should return the correct range for offsets") {
      eventually {
        val ranges = Offsets.offsetRange("testJson", -2, -1, cfg)
        ranges shouldBe Map(TopicAndPartition("testJson", 0) -> (0, 11))
      }
    }

    //todo: fix this, can't get it to work
    //    it("Should use the last offset properly") {
    //      Await.result(kafkaStarted, 15.seconds)
    //      //let's consume some msgs
    //      val consumerProperties = new Properties
    //      consumerProperties.put("zookeeper.connect", "localhost:5000")
    //      consumerProperties.put("group.id", "10")
    //      consumerProperties.put("socket.timeout.ms", "500")
    //      consumerProperties.put("consumer.id", "test")
    //      consumerProperties.put("auto.offset.reset", "smallest")
    //      consumerProperties.put("consumer.timeout.ms", "5000")
    //      val consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties))
    //      val stringDecoder = new StringDecoder(new VerifiableProperties(new Properties))
    //      val topicMap = Map("testJson" -> (1: java.lang.Integer)).asJava
    //      val s = consumer.createMessageStreams(topicMap, stringDecoder, stringDecoder)
    //      val it = s.get("testJson").get(0).iterator()
    //      for (i <- 0 to 10) {
    //        //consuming messages
    //        it.next()
    //      }
    //      consumer.commitOffsets()
    //      //produce another one
    //      kafka.sendMessages(new KeyedMessage[String, String]("testJson", "test", "test"))
    //      val ranges = Offsets.offsetRange("testJson", -3, -1, cfg)
    //      ranges shouldBe Map(TopicAndPartition("testJson", 0) -> (11, 12))
    //      consumer.shutdown()
    //    }
  }

}
