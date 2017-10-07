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


import hydra.spark.internal.Logging
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.groupon.metrics.SparkCounter
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

trait DFOperation extends Validatable with Logging {

  /**
    * A unique id identifying this operation.
    *
    * @return
    */
  def id: String = ClassUtils.getShortCanonicalName(getClass)

  def transform(df: DataFrame): DataFrame

  /**
    * Set of operation-specific properties to be included in metrics/reporting.
    */
  val operationProperties: Map[String, String] = Map.empty

  /**
    * Called once per Spark application, before the operations run.
    *
    */
  def preStart(): Unit = {}

  /**
    * Called by the framework when the stage for this operation has completed.
    * Once for batch jobs or after every micro batch for streaming jobs.
    *
    */
  def onStageCompleted(stageInfo: StageInfo): Unit = {}

  /**
    * Fired when the stage for this operation is submitted to the scheduler.
    *
    */
  def preTransform(): Unit = {}

  lazy val processedRowsCounter: SparkCounter = HydraMetrics.counter(id, "processedRows")

  /**
    * Can be overridden to intercept calls to `preStart`. Initializes the counters and calls `preStart` by default.
    */
  protected[hydra] final def aroundPreStart(): Unit = {
    preStart()
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

