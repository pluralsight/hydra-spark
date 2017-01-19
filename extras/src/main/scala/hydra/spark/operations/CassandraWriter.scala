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

package hydra.spark.operations

import com.typesafe.config.{ Config, ConfigFactory }
import hydra.spark.api._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{ AnalysisException, DataFrame }

import scala.util.Try

/**
 * Created by alexsilva on 8/15/16.
 */
case class CassandraWriter(keyspace: String, table: String, columns: List[CassandraColumnMapping]) extends DFOperation {

  private val mappingByTarget: Map[String, CassandraColumnMapping] = columns.map(m => m.name -> m).toMap

  override def id: String = s"cassandra-writer-$keyspace.$table"

  override def transform(df: DataFrame): DataFrame = {

    val targetCols = columns.map { f =>
      f.name -> Try(df(f.source)).recover { case t: AnalysisException => lit(null) }.get
    }.toMap

    val targetDf = targetCols.foldLeft(df)((df, c) => df.withColumn(c._1, c._2.cast(mappingByTarget(c._1).`type`)))

    targetDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save()

    df
  }

  override def validate: ValidationResult = checkRequiredParams(Seq("keyspace" -> keyspace, "table" -> table))

}

case class CassandraColumnMapping(source: String, name: String, `type`: DataType)

object CassandraWriter {
  def apply(cfg: Config): CassandraWriter = {
    import hydra.spark.configs._
    import hydra.spark.util.DataTypes._
    def mapping(cfg: Config) = {
      val name = cfg.get[String]("name").getOrElse(throw new IllegalArgumentException("A column name is required."))
      val source = cfg.get[String]("source").getOrElse(name)
      val theType = cfg.get[String]("type").getOrElse("string")
      CassandraColumnMapping(source, name, theType)
    }

    val keyspace = cfg.get[String]("keyspace").getOrElse("")
    val table = cfg.get[String]("table").getOrElse("")
    val columnList = cfg.get[List[Config]]("columns").getOrElse(List(ConfigFactory.empty()))
    val columns = columnList.map(mapping)
    CassandraWriter(keyspace, table, columns)
  }
}