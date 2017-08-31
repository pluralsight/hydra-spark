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


import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.base.CaseFormat
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

trait DFOperation extends Validatable {

  /**
    * A unique id identifying this operation.
    *
    * @return
    */
  def id: String = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, getClass.getSimpleName)

  def transform(df: DataFrame): DataFrame

  private val counters = new scala.collection.mutable.HashMap[String, LongAccumulator]()

  private[spark] val collectMetrics = new AtomicBoolean(false)

  def preStart(sparkContext: SparkContext): Unit = {}

  def postStop(): Unit = {}

  def processedRows = counters(id + "_PROCESSED_ROWS")

  def outputRows = counters(id + "_OUTPUT_ROWS")

  protected[hydra] def initCounters(sc: SparkContext) = {
    collectMetrics.set(sc.getConf.getBoolean("spark.metrics.hydra", true))
    counters.getOrElseUpdate(id + "_PROCESSED_ROWS", sc.longAccumulator(id + "_PROCESSED_ROWS"))
    counters.getOrElseUpdate(id + "_OUTPUT_ROWS", sc.longAccumulator(id + "_OUTPUT_ROWS"))
  }


  /**
    * Can be overridden to intercept calls to `preStart`. Initializes the counters and calls `preStart` by default.
    */
  protected[hydra] def aroundPreStart(sc: SparkContext): Unit = {
    initCounters(sc)
    preStart(sc)
  }


  def ifNotEmpty(df: DataFrame)(f: DataFrame => DataFrame): DataFrame = {
    Try(df.first) match {
      case Success(_) => f.apply(df)
      case Failure(x) => df
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
