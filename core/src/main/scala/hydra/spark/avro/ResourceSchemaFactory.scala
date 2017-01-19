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

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.springframework.core.io.DefaultResourceLoader

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by alexsilva on 1/3/17.
  */
object ResourceSchemaFactory extends SchemaFactory {

  val schemaCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[String, Schema]() {
        def load(key: String): Schema = {
          Await.result(loadSchema(key), 10.seconds)
        }
      })


  override def getValueSchema(location: String): Schema = {
    schemaCache.get(location)
  }

  def loadSchema(location: String): Future[Schema] = {
    Future {
      val resource = new DefaultResourceLoader().getResource(location)
      new Parser().parse(resource.getInputStream)
    }.recover {
      case e: Exception => throw new SchemaFactoryException(e)
    }
  }
}
