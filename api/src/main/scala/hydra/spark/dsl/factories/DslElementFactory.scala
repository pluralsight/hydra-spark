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

package hydra.spark.dsl.factories

import java.lang.reflect.Modifier

import com.google.common.base.CaseFormat
import com.typesafe.config.{ Config, ConfigObject }
import hydra.spark.api.{ DFOperation, Source }
import org.reflections.Reflections

import scala.collection.JavaConverters._

/**
 * Provides sources based on a config object.  This is a factory method pattern.
 *
 * Created by alexsilva on 10/21/16.
 */
trait DslElementFactory {

  import FactoryHelper._

  /**
   * The sources known by this factory.
   *
   * @return
   */
  protected def sources: Map[String, Class[_ <: Source[_]]]

  protected def operations: Map[String, Class[_ <: DFOperation]]

  def createSource(cfg: ConfigObject, properties: Config): Source[_] = {
    val fsources = materialize[Source[_]]("source", cfg, sources, properties)
    require(fsources.size == 1, "Only one source is allowed.")
    fsources.head
  }

  def createOperations(cfg: ConfigObject, properties: Config): Seq[DFOperation] =
    materialize[DFOperation]("operation", cfg, operations, properties)
}

case class ClasspathDslElementFactory(sourcePkg: Seq[String], operationsPkg: Seq[String]) extends DslElementFactory {

  private val reflections = new Reflections((sourcePkg ++ operationsPkg).asJava)

  override protected val sources: Map[String, Class[_ <: Source[_]]] = reflections.getSubTypesOf(classOf[Source[_]])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).map(x => sourceName(x) -> x).toMap

  override protected val operations: Map[String, Class[_ <: DFOperation]] = reflections
    .getSubTypesOf(classOf[DFOperation])
    .asScala.filterNot(c => Modifier.isAbstract(c.getModifiers)).map(x => opName(x) -> x).toMap

  private def sourceName(clz: Class[_]) = {
    val name = clz.getSimpleName
    CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, name.substring(0, name.lastIndexOf("Source")))
  }

  private def opName(clz: Class[_]) = {
    CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, clz.getSimpleName)
  }

}

