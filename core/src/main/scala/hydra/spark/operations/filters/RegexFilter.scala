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

package hydra.spark.operations.filters

import hydra.spark.api.{ Invalid, ValidationError, _ }
import org.apache.spark.sql.DataFrame

/**
 * Created by alexsilva on 6/24/16.
 */
case class RegexFilter(field: String, expr: String) extends DFOperation {
  override def id: String = s"RegexFilter-$field-$expr"

  override def transform(df: DataFrame): DataFrame = {
    val condition = df(field).rlike(expr)
    df.filter(condition)
  }

  override def validate: ValidationResult = {
    if (!Seq(Option(if (field.isEmpty) null else field), Option(if (expr.isEmpty) null else expr)).flatten.isEmpty)
      Invalid(ValidationError("regex-filter", "Both field and expr are required."))
    else
      Valid
  }
}