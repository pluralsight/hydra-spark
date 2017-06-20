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

import hydra.spark.api._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * DBTable can be either a table name or a query (with an alias.)
 *
 * Created by alexsilva on 7/21/16.
 */
case class JdbcSource(dbtable: String, properties: Map[String, String]) extends RowSource {
  override def name: String = "jdbcsource"

  private val theProps = properties + ("dbtable" -> dbtable)

  //TODO: implement this
  override def createStream(sc: StreamingContext): DStream[Row] =
    throw new InvalidDslException("Jdbc Source does not support streaming.")

  override def createDF(ctx: SparkSession): DataFrame = {
    ctx.read.format("jdbc").options(theProps).load()
  }

  override def validate: ValidationResult = {
    if (Seq(properties.get("url"), Option(if (dbtable.isEmpty()) null else dbtable)).flatten.size != 2)
      Invalid(ValidationError(name, "A table name and a url property are required"))
    else
      Valid
  }

}
