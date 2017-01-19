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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema

import scala.util.Try

/**
 * Created by alexsilva on 1/3/17.
 */
case class RegistrySchemaFactory(client: SchemaRegistryClient) extends SchemaFactory {

  override def getValueSchema(subject: String): Schema = {
    val schemaSubject = if (!subject.endsWith("-value")) subject + "-value" else subject
    Try(client.getLatestSchemaMetadata(schemaSubject)).map(m => client.getByID(m.getId)).recover {
      case e: Exception => throw new SchemaFactoryException(e)
    }.get

  }
}
