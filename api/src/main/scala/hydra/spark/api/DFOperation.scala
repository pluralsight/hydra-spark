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
import org.apache.spark.sql.DataFrame

trait DFOperation extends Validatable {

  /**
   * A unique id indentifying this operation.
   *
   * @return
   */
  def id: String = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, getClass.getSimpleName)

  def transform(df: DataFrame): DataFrame

  def checkRequiredParams(params: Seq[(String, Any)]): ValidationResult = {
    val nullParams = params.collect { case (n, "") => n case (n, null) => n }

    val result = Either.cond(
      nullParams.isEmpty, Valid, s"The following parameter(s) are required:${nullParams.mkString(", ")}"
    )

    if (result.isLeft) Invalid(ValidationError(id, result.left.get)) else result.right.get
  }
}
