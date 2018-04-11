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

package hydra.spark.app.parser

import java.util.UUID

import com.typesafe.config._
import configs.syntax._
import hydra.spark.api._
import hydra.spark.configs._
import hydra.spark.app.factories.ClasspathDslElementFactory
import hydra.spark.internal.Logging

import scala.util.Try

case class TypesafeDSLParser(sourcesPkg: Seq[String] = Seq("hydra.spark.sources"),
                             operationsPkg: Seq[String] = Seq("hydra.spark.operations"))
  extends DSLParser with Logging {

  val factory = ClasspathDslElementFactory(sourcesPkg, operationsPkg)

  override def parse(dsl: String): Try[TransformationDetails[_]] = {
    parse(ConfigFactory.parseString(dsl, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)))
  }

  def parse(dsl: Config): Try[TransformationDetails[_]] = {

    Try(dsl.resolve()).map { transport =>

      val source = transport.get[ConfigObject]("source")
        .map(s => factory.createSource(s, transport))
        .valueOrThrow(e => InvalidDslException(s"Invalid DSL: ${e.head.throwable.getMessage}"))

      val operations: Seq[DFOperation] = transport.get[ConfigObject]("operations")
        .map(ops => factory.createOperations(ops, transport))
        .valueOrThrow(e => InvalidDslException(s"Invalid DSL: ${e.head.throwable.getMessage}"))

      val name = transport.get[String]("name").valueOrElse(UUID.randomUUID().toString)

      val streamingProps = transport.flattenAtKey("streaming")

      val isStreaming = streamingProps.get("streaming.interval").isDefined

      TransformationDetails(name, source, operations, isStreaming, dsl)
    }
  }
}