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
import hydra.spark.api._
import hydra.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by alexsilva on 6/18/16.
  */
case class DatabaseUpsert(table: String, properties: Map[String, String],
                          idColumn: Option[ColumnMapping], columns: Seq[ColumnMapping]) extends DFOperation with Logging {

  val mapping = TableMapping(idColumn, columns)

  override def transform(df: DataFrame): DataFrame = {
    ifNotEmpty(df) { df =>
      val ndf = mapping.targetDF(df)

      val idField = idColumn.map(id => StructField(id.target, id.`type`, nullable = false))

      ndf.write.mode(properties.get("savemode").getOrElse("append")).upsert(properties("url"), table, idField,
        new JDBCOptions(properties), ndf)
      ndf
    }
  }

  override def validate: ValidationResult = {

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
    import configs.syntax._
    import hydra.spark.util.DataTypes._
    def mapping(cfg: Config): ColumnMapping = {
      val name = cfg.get[String]("name").valueOrThrow(_ => new IllegalArgumentException("A column name is required."))
      val source = cfg.get[String]("source").valueOrElse(name)
      val theType = cfg.get[String]("type").valueOrElse("string")
      ColumnMapping(source, name, theType)
    }

    val properties = cfg.get[Map[String, String]]("properties").valueOrElse(Map.empty[String, String])
    val table = cfg.get[String]("table").valueOrThrow(_ => new IllegalArgumentException("Table is required for jdbc"))
    val idColumn = cfg.get[Config]("idColumn").map(v => mapping(v)).toOption
    val columnList = cfg.get[List[Config]]("columns").valueOrElse(Seq.empty)
    val columns = columnList.map(mapping)

    DatabaseUpsert(table, properties, idColumn, columns)
  }
}

case class ColumnMapping(source: String, target: String, `type`: DataType)

