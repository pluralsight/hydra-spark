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

import hydra.spark.api.{Invalid, ValidationError, _}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Created by alexsilva on 6/24/16.
 */
case class ValueFilter(column: String, value: Any, operation: String) extends DFOperation {
  override def id: String = s"value-filter-$column-$value"

  val filterMap = Map[String, DataFrame => Column](
    ">" -> (df => df(column) > value),
    ">=" -> (df => df(column) >= value),
    "<" -> (df => df(column) < value),
    "<=" -> (df => df(column) <= value),
     "=" -> (df => df(column) === value)
  )

  override def transform(df: DataFrame): DataFrame = {
    filterMap.get(operation).map(_.apply(df)).map(df.filter).getOrElse(
      throw InvalidDslException("Provided operation parameter is not supported."))
  }

  override def validate: ValidationResult = {
    if (Seq(Option(column), Option(value), Option(operation)).flatten.size < 3)
      Invalid(ValidationError("value-filter", "Column, value, and operation are required."))
    else if (!filterMap.contains(operation))
      Invalid(ValidationError("value-filter", s"Operation $operation is not supported."))
    else
      Valid
  }
}
