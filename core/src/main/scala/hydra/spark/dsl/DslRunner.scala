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

package hydra.spark.dsl

import hydra.spark.api._
import hydra.spark.dispatch.SparkDispatch
import org.apache.spark.SparkException

import scala.language.existentials

/**
  * Runs a Hydra Spark job in 'stand alone' mode.
  *
  * Created by alexsilva on 10/25/16.
  */
object DslRunner extends App {

  val dsl = args(0)

  runJob(dsl)

  def runJob(dsl: String) = {

    val sparkDispatch = SparkDispatch(dsl)

    sparkDispatch.validate match {
      case Invalid(errors) =>
        throw new SparkException(errors.map(_.message).mkString(";"))
      case Valid =>
        sparkDispatch.run()
        sparkDispatch.awaitTermination()
        sparkDispatch.stop()
    }
  }
}