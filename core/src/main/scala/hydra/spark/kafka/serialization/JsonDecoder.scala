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

import com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
 * Created by alexsilva on 12/3/16.
 */
class JsonDecoder(props: VerifiableProperties = null) extends Decoder[JsonNode] {

  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  val mapper = new ObjectMapper()

  override def fromBytes(bytes: Array[Byte]): JsonNode = {
    val json = new String(bytes, encoding)
    mapper.reader().readTree(json)
  }
}

