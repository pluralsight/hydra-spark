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

import com.typesafe.config.Config
import hydra.spark.api.{ DFOperation, Valid, ValidationResult }
import org.apache.spark.sql.DataFrame

/**
 * Created by alexsilva on 8/12/16.
 */
case class CountRecords(column: String) extends DFOperation {

  override def id: String = "count-records"

  override def transform(df: DataFrame): DataFrame = {
    val count = df.count()
    df.sqlContext.createDataFrame(Seq(Count(count)))
  }

  /**
   * Validates the target configuration as a future.
   *
   * @return
   */
  override def validate: ValidationResult = Valid
}

object CountRecords {
  def apply(cfg: Config): CountRecords = {
    CountRecords(cfg.getString("column"))
  }
}

case class Count(count: Long)