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

package hydra.spark.testutils

/**
 * Created by alexsilva on 1/9/17.
 */

import hydra.spark.sources.kafka.KafkaMessageAndMetadata
import info.batey.kafka.unit.KafkaUnit
import kafka.api.OffsetRequest
import kafka.producer.KeyedMessage
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, Suite }

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedKafka extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  @transient private var _kafka: KafkaUnit = _

  def kafka: KafkaUnit = _kafka

  val avroTopic = Map("format" -> "json", "start" -> OffsetRequest.EarliestTime)
  val jsonTopic = Map("format" -> "string", "start" -> OffsetRequest.EarliestTime)

  val topics: Map[String, Map[String, Any]] = Map(
    "testJson" -> avroTopic,
    "testAvro" -> jsonTopic
  )

  val schema = Source.fromInputStream(getClass.getResourceAsStream("/schema.avsc")).mkString

  val bootstrapServers = "localhost:5001"

  val zkConnect = "localhost:5000"

  private type KMMD = KafkaMessageAndMetadata[_, _]

  val jsonMessages = new ArrayBuffer[String]()
  val avroMessages = new ArrayBuffer[String]()

  override def beforeAll() {
    super.beforeAll()
    _kafka = new KafkaUnit(5000, 5001)
    _kafka.setKafkaBrokerConfig("num.partitions", "1")

    doStartKafka()
  }

  override def afterAll() {
    try {
      kafka.shutdown()
    } finally {
      super.afterAll()
    }
  }

  private def doStartKafka(): Boolean = {
    kafka.startup()
    kafka.createTopic("testJson")
    kafka.createTopic("testAvro")
    kafka.createTopic("__consumer_offsets")
    for (i <- 0 to 10) {
      val json = s"""{"no": "$i","value":"hello"}"""
      val jsonMsg = new KeyedMessage[String, String]("testJson", i.toString, json)
      val avroMsg = new KeyedMessage[String, String]("testAvro", i.toString, json)
      jsonMessages += json
      kafka.sendMessages(jsonMsg)
      kafka.sendMessages(avroMsg)
    }

    true
  }
}
