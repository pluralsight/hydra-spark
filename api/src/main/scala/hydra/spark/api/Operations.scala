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

import org.apache.spark.sql.DataFrame

/**
 * Created by alexsilva on 7/13/16.
 */
case class Operations(steps: Seq[DFOperation]) {
  /**
   * @return a new merged `Operations`
   */
  def ++(other: Operations): Operations = Operations(this.steps ++ other.steps)

}

object Operations {
  def apply(operation: DFOperation): Operations = new Operations(Seq(operation))

  def apply(operation: (DataFrame) => DataFrame, id: String): Operations =
    new Operations(Seq(FunDFOperation(operation, id)))
}

case class FunDFOperation(operation: (DataFrame) => DataFrame, override val id: String) extends DFOperation {
  override def transform(df: DataFrame): DataFrame = operation.apply(df)

  override def validate: ValidationResult = Valid
}