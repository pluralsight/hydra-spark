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

package hydra.spark.operations.jdbc

import java.security.MessageDigest

import com.typesafe.config.Config
import hydra.spark.util.Collections._
import hydra.spark.api._
import hydra.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ AnalysisException, Column, DataFrame }

import scala.collection.mutable
import scala.util.Try

/**
 * Created by alexsilva on 6/18/16.
 */
case class DatabaseUpsert(table: String, properties: Map[String, String],
    idColumn: Option[SQLMapping], columns: Seq[SQLMapping]) extends DFOperation with Logging {

  val mapping = TableMapping(idColumn, columns)

  override def transform(df: DataFrame): DataFrame = {
    val ndf = prepareDF(df)

    val idField = idColumn.map(id => StructField(id.target, id.`type`, nullable = false))

    ndf.write.mode(properties.get("savemode").getOrElse("append")).upsert(properties("url"), table, idField,
      properties, ndf)

    ndf
  }

  def prepareDF(df: DataFrame): DataFrame = {
    val targetDf = mapping.targetDf(df)
    targetDf
  }

  override def validate: ValidationResult = {
    import DatabaseUpsert._

    val errors = mutable.ListBuffer[ValidationError]()
    val requiredKeys = "url"
    if (properties.filterKeys(_.matches(requiredKeys)).size == 0)
      errors += ValidationError("database-upsert", "Url and user are required properties.", properties)
    if (errors.size > 0) Invalid(errors) else Valid
  }

  override val id: String = {
    val idString = table + properties.map(k => k._1 + "->" + k._2).mkString +
      idColumn.map(_.toString) + columns.map(_.toString).mkString
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(idString.getBytes).map("%02x".format(_)).mkString
  }
}

object DatabaseUpsert {

  def apply(cfg: Config): DatabaseUpsert = {
    import hydra.spark.configs._
    import hydra.spark.util.DataTypes._
    def mapping(cfg: Config): SQLMapping = {
      val name = cfg.get[String]("name").getOrElse(throw new IllegalArgumentException("A column name is required."))
      val source = cfg.get[String]("source").getOrElse(name)
      val theType = cfg.get[String]("type").getOrElse("string")
      SQLMapping(source, name, theType)
    }

    val properties = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty[String, String])
    val table = cfg.get[String]("table").getOrElse(throw new IllegalArgumentException("Table is required for jdbc"))
    val idColumn = cfg.get[Config]("idColumn").map(v => mapping(v))
    val columnList = cfg.get[List[Config]]("columns").getOrElse(Seq.empty)
    val columns = columnList.map(mapping)

    DatabaseUpsert(table, properties, idColumn, columns)
  }
}

case class SQLMapping(source: String, target: String, `type`: DataType)

private[jdbc] case class TableMapping(idColumn: Option[SQLMapping], columns: Seq[SQLMapping]) {

  val mapping = idColumn.toList ++ columns

  val mappingByTarget: Map[String, SQLMapping] = mapping.map(m => m.target -> m).toMap

  val targetSchema: Seq[StructField] = mapping.map(m => StructField(m.target, m.`type`))

  /**
   * Converts the source data frame into the target dataframe given the mapping
   *
   * @param df
   */
  def targetDf(df: DataFrame): DataFrame = {
    val tdf = targetCols(df)
    df.select(tdf: _*)
  }

  private def targetCols(df: DataFrame): Seq[Column] = {
    val target = targetSchema.map { f =>
      val mapping = mappingByTarget(f.name)
      Try(df(mapping.source)).recover { case t: AnalysisException => lit(null) }
        .get.as(mapping.target).cast(mapping.`type`)
    }

    if (target.isEmpty) inferTargetColumns(df) else target
  }

  private def inferTargetColumns(df: DataFrame): Seq[Column] = {
    val fcols = flattenSchema(df.schema)
    val fs = fcols.map(c => df(c.toString()).as(c.toString().replace(".", "_")))
    fs
  }

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    import org.apache.spark.sql.functions._
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }
}