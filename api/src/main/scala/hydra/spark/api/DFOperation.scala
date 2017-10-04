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


import com.google.common.base.CaseFormat
import hydra.spark.internal.Logging
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

trait DFOperation extends Validatable with Logging {

  /**
    * A unique id identifying this operation.
    *
    * @return
    */
  def id: String = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, getClass.getSimpleName)

  def transform(df: DataFrame): DataFrame

  /**
    * Called once per Spark application, before the operations run.
    *
    * @param hydraContext
    */
  def preStart(hydraContext: HydraContext): Unit = {}

  /**
    * Called by the framework when the stage for this operation has completed.
    * Once for batch jobs or after every micro batch for streaming jobs.
    *
    * Returns a map of operation-specific properties to the operation (such as URL, kafka brokers, etc.).
    */
  def onStageCompleted(hydraContext: HydraContext): Map[String, String] = Map.empty

  /**
    * Fired when the stage for this operation is submitted to the scheduler.
    *
    * @param hydraContext
    */
  def preTransform(hydraContext: HydraContext): Unit = {}


  //set of "standard" counters
  var processedRows: LongAccumulator = _

  var processedRowsCounter: LongAccumulator = _

  /**
    * Can be overridden to intercept calls to `preStart`. Initializes the counters and calls `preStart` by default.
    */
  protected[hydra] final def aroundPreStart(hydraContext: HydraContext): Unit = {
    processedRowsCounter = hydraContext.metrics.getOrCreateCounter(getClass, "processedRows")
    preStart(hydraContext)
  }

  def ifNotEmpty(df: DataFrame)(f: DataFrame => DataFrame): DataFrame = {
    Try(df.first) match {
      case Success(_) => f.apply(df)
      case Failure(_) => df
    }
  }

  override def validate: ValidationResult = Valid

  def checkRequiredParams(params: Seq[(String, Any)]): ValidationResult = {
    val name = ClassUtils.getShortCanonicalName(getClass())
    val nullParams = params.collect { case (n, "") => n case (n, null) => n }

    val result = Either.cond(
      nullParams.isEmpty, Valid, s"The following parameter(s) are required for $name: ${nullParams.mkString(", ")}"
    )

    if (result.isLeft) Invalid(ValidationError(id, result.left.get)) else result.right.get
  }

}

