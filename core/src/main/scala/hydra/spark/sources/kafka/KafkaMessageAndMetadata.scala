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

import kafka.message.MessageAndMetadata

/**
 * Value class to avoid java.io.NotSerializableException for kafka.message.MessageAndMetadata
 */
case class KafkaMessageAndMetadata[K, V](key: K, value: V, topic: String,
  partition: Int, offset: Long) extends Serializable

object KafkaMessageAndMetadata {
  def apply[K, V](mmd: MessageAndMetadata[K, V]) = {
    new KafkaMessageAndMetadata[K, V](mmd.key, mmd.message, mmd.topic, mmd.partition, mmd.offset)
  }
}