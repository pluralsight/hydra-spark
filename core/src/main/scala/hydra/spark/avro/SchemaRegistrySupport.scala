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

package hydra.spark.avro

import io.confluent.kafka.schemaregistry.client.{ CachedSchemaRegistryClient, MockSchemaRegistryClient }
import org.apache.avro.Schema

/**
 * Created by alexsilva on 12/7/16.
 */
trait SchemaRegistrySupport {

  def properties: Map[String, String]

  lazy val schemaRegistryClient = properties.get("schema.registry.url") match {
    case Some(url) => new CachedSchemaRegistryClient(url, 10)
    case None => new MockSchemaRegistryClient()
  }

  def getValueSchema(defaultName: String): Schema = {
    val location = properties.get("schema").getOrElse(defaultName)
    val fact = if (location.indexOf(":") != -1) ResourceSchemaFactory else RegistrySchemaFactory(schemaRegistryClient)
    fact.getValueSchema(location)
  }
}
