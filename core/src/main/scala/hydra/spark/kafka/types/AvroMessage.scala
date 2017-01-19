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

import com.pluralsight.hydra.common.avro.JsonConverter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
 * Created by alexsilva on 10/30/15.
 */
case class AvroMessage(schema: Schema, key: String, json: String)
    extends KafkaMessage[String, GenericRecord] {

  val payload: GenericRecord = {
    val converter: JsonConverter[GenericRecord] = new JsonConverter[GenericRecord](schema)
    converter.convert(json)
  }
}
