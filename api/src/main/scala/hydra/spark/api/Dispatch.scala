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

package hydra.spark.api

import java.security.MessageDigest

import com.typesafe.config._

import scala.language.existentials

/**
  * Created by alexsilva on 6/17/16.
  */
trait Dispatch[S] extends Validatable {

  /**
    * A unique id identifying this dispatch job.  It is important it is unique but reproducible because it is used in
    * Spark checkpointing.
    *
    * @return
    */
  val id: String = {
    val idString = source.name + operations.steps.map(_.id).mkString("-")
    val digest = MessageDigest.getInstance("MD5")
    "hydra-" + digest.digest(idString.getBytes).map("%02x".format(_)).mkString
  }

  /**
    * A user-provided name for this dispatch. (Optional)
    * If none is provided, the id is used.
    *
    * @return
    */
  def name: String = id

  def source: Source[S]

  def operations: Operations

  def run(): Unit

  def awaitTermination(): Unit = {}

  def stop(): Unit


  /**
    * The dispatch properties, or all entries defined within the dispatch element in the DSL.
    *
    * @return
    */
  def dsl: Config

  override def validate: ValidationResult = {
    val validation = operations.steps.map(_.validate) :+ source.validate
    val errors: Seq[ValidationError] = validation.flatMap {
      case Invalid(e) => e
      case Valid => None
    }

    errors match {
      case Nil => Valid
      case e => Invalid(e)
    }
  }
}