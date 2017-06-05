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
import kafka.consumer.ConsumerConfig
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 12/12/16.
  */
class OffsetsSpec extends Matchers with FunSpecLike with Eventually with KafkaTestSupport
with BeforeAndAfterAll {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  val props = new Properties()
  props.putAll(Map(
    "bootstrap.servers" -> bootstrapServers,
    "zookeeper.connect" -> zkConnect,
    "group.id" -> "hydra-spark").asJava)

  val cfg = new ConsumerConfig(props)

  override def beforeAll() = {
    super.beforeAll()
    startKafka()
  }

  describe("Kafka offsets") {
    it("Should translate topic start/stop values into the correct number") {
      var properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "largest")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.LatestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "smallest")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "-2")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.EarliestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "-1")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe OffsetRequest.LatestTime
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "1243345")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe 1243345
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "NaN")
      intercept[InvalidDslException] {
        Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime)
      }

      properties = Map("bootstrap.servers" -> "localhost:6001", "start" -> "last")
      Offsets.stringToNumber(properties.get("start"), OffsetRequest.EarliestTime) shouldBe -3
      Offsets.stringToNumber(properties.get("stop"), OffsetRequest.LatestTime) shouldBe OffsetRequest.LatestTime
    }

    it("Should return the correct range for offsets") {
      eventually {
        val ranges = Offsets.offsetRange("testJson", -2, -1, cfg)
        ranges shouldBe Map(new TopicPartition("testJson", 0) -> (0, 11))
      }
    }

    //todo: fix this, can't get it to work
    ignore("Should use the last offset properly") {
      for (i <- 0 until 10) {
        val json = s"""{"messageId": $i,"messageValue":"hello"}"""
        EmbeddedKafka.publishStringMessageToKafka("test_offset", json)
      }
      EmbeddedKafka.consumeNumberStringMessagesFrom("test_offset", 10, true)
      EmbeddedKafka.publishStringMessageToKafka("test_offset", "test")
      val ranges = Offsets.offsetRange("testJson", -3, -1, cfg)
      ranges shouldBe Map(new TopicPartition("test_offset", 0) -> (11, 12))

    }
  }

}
