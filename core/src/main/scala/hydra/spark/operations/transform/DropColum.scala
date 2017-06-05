package hydra.spark.operations.transform

/**
  * Created by alexsilva on 6/5/17.
  */
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

import hydra.spark.api.DFOperation
import org.apache.spark.sql.DataFrame

/**
  * Created by alexsilva on 8/16/16.
  */
case class DropColumn(names: Seq[String]) extends DFOperation {
  override def id: String = s"drop-column-$names"

  override def transform(df: DataFrame): DataFrame = {
    df.drop(names:_*)
  }

  override def validate: ValidationResult = {
    checkRequiredParams(Seq(("names", names)))
  }
}