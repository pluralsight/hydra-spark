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

import com.typesafe.config.Config
import hydra.spark.api._
import hydra.spark.operations.common.{ColumnMapping, TableMapping}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.DataFrameWriterExtensions
import org.apache.spark.sql.types._

/**
  * Created by alexsilva on 6/18/16.
  */
case class DatabaseUpsert(table: String, url: String, properties: Map[String, String],
                          idColumn: Option[ColumnMapping], columns: Seq[ColumnMapping]) extends DFOperation
  with DataFrameWriterExtensions with JdbcHelper {

  val mapping = TableMapping(idColumn, columns)

  private val jdbcOptions = new JDBCOptions(url, table, properties)

  private[jdbc] override lazy val connectionFactory = JdbcUtils.createConnectionFactory(jdbcOptions)

  override def preTransform(hydraContext: HydraContext): Unit = {
    hydraContext.metrics.getOrCreateCounter(getClass, "initialTableRows").add(getRowCount(table).getOrElse(0L))
  }

  override def transform(df: DataFrame): DataFrame = {
    ifNotEmpty(df) { df =>
      val ndf = mapping.targetDF(df)
      val idField = idColumn.map(id => StructField(id.target, id.`type`, nullable = false))
      ndf.write.mode(properties.get("savemode").getOrElse("append")).upsert(idField, jdbcOptions, ndf)
      ndf
    }
  }

  override def validate: ValidationResult = {
    checkRequiredParams(Seq("url" -> url, "table" -> table, "user" -> properties.get("user").getOrElse(null)))
  }

  override val id: String = s"${super.id}-${table.toLowerCase()}"

  override def onStageCompleted(hydraContext: HydraContext): Map[String, String] = {
    val dbRows = getRowCount(table).getOrElse(0L)
    hydraContext.metrics.getOrCreateCounter(getClass, "finalTableRows").add(dbRows)
    Map("table" -> table, "isUpsert" -> idColumn.isDefined.toString, "url" -> url)
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
    val table = cfg.get[String]("table")
      .valueOrThrow(_ => new IllegalArgumentException("Table is required for DatabaseUpsert"))
    val url = cfg.get[String]("url")
      .valueOrThrow(_ => new IllegalArgumentException("url is required for DatabaseUpsert"))
    val idColumn = cfg.get[Config]("idColumn").map(v => mapping(v)).toOption
    val columnList = cfg.get[List[Config]]("columns").valueOrElse(Seq.empty)
    val columns = columnList.map(mapping)

    DatabaseUpsert(table, url, properties, idColumn, columns)
  }
}



