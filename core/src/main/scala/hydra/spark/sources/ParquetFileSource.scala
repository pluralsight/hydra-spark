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

package hydra.spark.sources

import hydra.spark.api.{ Invalid, Source, Valid, ValidationResult }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.util.Try

/**
 * Created by alexsilva on 7/21/16.
 */
class ParquetFileSource(file: String) extends RowSource {
  override def name: String = "parquet-source"

  override def createStream(sc: StreamingContext): DStream[Row] = ???

  override def createDF(ctx: SQLContext): DataFrame = {
    ctx.read.parquet("people.parquet")
  }

  /**
   * Validates the target configuration as a future.
   *
   * @return
   */
  override def validate: ValidationResult = {
    Try(require(file.length > 0, "A file is required.")).map(x => Valid)
      .recover { case t: Throwable => Invalid(name, t.getMessage) }.get
  }
}
