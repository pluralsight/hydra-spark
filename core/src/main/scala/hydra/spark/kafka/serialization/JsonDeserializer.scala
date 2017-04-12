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

package hydra.spark.kafka.serialization

import java.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.common.serialization.Deserializer

/**
  * Created by alexsilva on 12/3/16.
  */
class JsonDeserializer extends Deserializer[JsonNode] {
  private var encoding = "UTF8"
  val mapper = new ObjectMapper()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    encoding = configs.get("serializer.encoding", "UTF8").toString
  }

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): JsonNode = {
    val json = new String(data, encoding)
    mapper.reader().readTree(json)
  }
}

