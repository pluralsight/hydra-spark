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

package hydra.spark.util

/**
 * Created by alexsilva on 12/15/15.
 */

import java.util.Properties

import kafka.utils.VerifiableProperties

object SimpleConsumerConfig {
  def apply(originalProps: Properties): SimpleConsumerConfig =
    SimpleConsumerConfig(new VerifiableProperties(originalProps))

  val defaultSocketTimeout = 30 * 1000
  val defaultSocketBufferSize = 64 * 1024
  val defaultFetchSize = 1024 * 1024
  val defaultClientId = "Hydra"
  val defaultMetadataBrokerList = "localhost:9092"
  val defaultRefreshLeaderBackoffMs = 200
  val defaultRefreshLeaderMaxRetries = 4
  val defaultKeyDecoder = "kafka.serializer.StringDecoder"
  val defaultValueDecoder = "kafka.serializer.StringDecoder"
}

case class SimpleConsumerConfig private (val props: VerifiableProperties) {

  import SimpleConsumerConfig._

  /** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
  val socketTimeoutMs = props.getInt("socket.timeout.ms", defaultSocketTimeout)

  /** the socket receive buffer for network requests */
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", defaultSocketBufferSize)

  /** the number of byes of messages to attempt to fetch */
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", defaultFetchSize)

  /** specified by the kafka consumer client, used to distinguish different clients */
  val clientId = props.getString("client.id", defaultClientId)

  /** this is for bootstrapping and the consumer will only use it for getting metadata */
  val metadataBrokerList = props.getString("metadata.broker.list", defaultMetadataBrokerList)

  /** backoff time to wait before trying to determine the leader of a partition that has just lost its leader */
  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", defaultRefreshLeaderBackoffMs)

  /** maximum attempts to determine leader for all partition before giving up */
  val refreshLeaderMaxRetries = props.getInt("refresh.leader.max.retries", defaultRefreshLeaderMaxRetries)

  val valueDecoder = props.getString("value.deserializer", defaultValueDecoder)

  val keyDecoder = props.getString("key.deserializer", defaultKeyDecoder)

  val groupId = props.getString("group.id")

  props.verify()
}
