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

import hydra.spark.internal.Logging
import hydra.spark.util.ReflectionUtils
import org.reflections.Reflections

import scala.collection.JavaConverters._


object Parsers extends Logging {

  private val reflections = new Reflections(Seq("hydra.spark").asJava)

  private val parsers = reflections.getSubTypesOf(classOf[DSLParser]).asScala
    .map(c => ReflectionUtils.objectOf(c))

  def forDSL(dsl: String): Option[DSLParser] = parsers.find(_.supports(dsl))

}