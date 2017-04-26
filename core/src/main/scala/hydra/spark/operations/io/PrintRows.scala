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

package hydra.spark.operations.io

import com.typesafe.config.Config
import hydra.spark.api._
import org.apache.spark.sql.DataFrame

/**
  * Created by alexsilva on 12/29/16.
  */
case class PrintRows(numRows: Int = 10) extends DFOperation {
  override def transform(df: DataFrame): DataFrame = {
    df.show(numRows)
    df
  }

  /**
    * Validates the target configuration as a future.
    *
    * @return
    */
  override def validate: ValidationResult = if (numRows > 0) Valid else
    Invalid(ValidationError("print-rows", "numRows needs to be greater than zero."))
}

object PrintRows {
  def apply(cfg: Config): PrintRows = {
    import configs.syntax._
    val numRows = cfg.get[Int]("numRows").valueOrElse(10)
    PrintRows(numRows)
  }
}