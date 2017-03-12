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

package hydra.spark.dsl.parser

import java.net.URL
import java.util.UUID

import com.typesafe.config._
import hydra.spark.api._
import hydra.spark.configs._
import hydra.spark.dispatch.context.{DefaultSparkContextFactory, StreamingContextFactory}
import hydra.spark.dsl.factories.ClasspathDslElementFactory
import hydra.spark.internal.Logging
import hydra.spark.util.ContextURLClassLoader

case class TypesafeDSLParser(sourcesPkg: Seq[String] = Seq("hydra.spark.sources"),
                             operationsPkg: Seq[String] = Seq("hydra.spark.operations"))
  extends DSLParser with Logging {

  val factory = ClasspathDslElementFactory(sourcesPkg, operationsPkg)

  val defaults = ConfigFactory.defaultReference.withFallback(ConfigFactory.load(getClass.getClassLoader, "reference"))

  override def parse(dsl: String): DispatchDetails[_] = {
    apply(ConfigFactory.parseString(dsl, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)))
  }

  def apply(dsl: Config): DispatchDetails[_] = {

    val transport = dsl.getConfig("transport").resolve()

    val source = transport.get[ConfigObject]("source") match {
      case Some(source) => factory.createSource(source, transport)
      case None => throw InvalidDslException("Invalid DSL: A source is required.")
    }

    val operations: Seq[DFOperation] = transport.get[ConfigObject]("operations") match {
      case Some(ops) => factory.createOperations(ops, transport)
      case None => throw InvalidDslException("Invalid DSL: At least one target/operation is required.")
    }

    val name = transport.get[String]("name").getOrElse(UUID.randomUUID().toString)

    val streamingProps = transport.flattenAtKey("streaming")

    val isStreaming = streamingProps.get("streaming.interval").isDefined

    val contextFactory = getContextFactory(transport, isStreaming)

    DispatchDetails(name, source, Operations(operations), isStreaming, dsl, contextFactory)
  }

  def getContextFactory(config: Config, isStreaming: Boolean): SparkContextFactory = {
    val factory = config.get[String]("context-factory") match {
      case Some(factoryClassName) =>
        val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)
        val factoryClass = jarLoader.loadClass(factoryClassName)
        Thread.currentThread.setContextClassLoader(jarLoader)
        factoryClass.newInstance.asInstanceOf[SparkContextFactory]
      case None => if (isStreaming) new StreamingContextFactory else new DefaultSparkContextFactory
    }
    factory
  }


}