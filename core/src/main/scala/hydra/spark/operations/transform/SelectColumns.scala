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

package hydra.spark.operations.transform

import hydra.spark.api._
import org.apache.spark.sql.DataFrame


/**
  * Created by alexsilva on 8/16/16.
  */
case class SelectColumns(columns: Seq[String]) extends DFOperation {
  override def id: String = s"select-column-${columns.mkString}"

  override def transform(df: DataFrame): DataFrame = {
    df.select(columns.head, columns.drop(1): _*)
  }

  override def validate: ValidationResult = {
    if (columns.isEmpty)
      Invalid(ValidationError("select-columns", "Column list cannot be empty."))
    else Valid
  }


}



