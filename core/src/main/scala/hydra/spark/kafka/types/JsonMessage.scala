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

package hydra.spark.kafka.types

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Created by alexsilva on 11/30/15.
 *
 * A Jackson backed JSON Kafka message implementation, where the key is a string object and the payload is a String
 * converted using Jackson.
 */
case class JsonMessage(key: String, payload: String) extends KafkaMessage[String, String]

object JsonMessage {
  val mapper = new ObjectMapper()

  def apply(key: String, obj: Any) = {
    val payload: String = mapper.writeValueAsString(obj)
    new JsonMessage(key, payload)
  }
}
