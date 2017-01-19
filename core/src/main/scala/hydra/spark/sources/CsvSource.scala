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

import com.typesafe.config.Config
import hydra.spark.api._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.util.Try

/**
 * Created by alexsilva on 8/16/16.
 */
case class CsvSource(path: String, header: Boolean = false) extends RowSource {
  override def name: String = "csv"

  override def createStream(sc: StreamingContext): DStream[Row] =
    throw new InvalidDslException("CSV source does not support streaming.")

  override def createDF(ctx: SQLContext): DataFrame = {
    val df = ctx.read
      .format("com.databricks.spark.csv")
      .option("header", header.toString) // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(path)
    df
  }

  override def validate: ValidationResult = {
    Try(require(path.length > 0, "A path is required.")).map(x => Valid)
      .recover { case t: Throwable => Invalid(name, t.getMessage) }.get
  }
}

object CsvSource {
  def apply(cfg: Config): CsvSource = {
    import hydra.spark.configs._
    val header = cfg.get[Boolean]("header").getOrElse(false)
    CsvSource(cfg.getString("path"), header)
  }
}
