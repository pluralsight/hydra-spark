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

import java.sql.SQLException

import com.typesafe.config.Config
import hydra.spark.api._
import hydra.spark.operations.common.{ColumnMapping, TableMapping}
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.DataFrameWriterExtensions
import org.apache.spark.sql.types._

/**
  * Created by alexsilva on 6/18/16.
  */
case class DatabaseUpsert(table: String, url: String, properties: Map[String, String],
                          idColumn: Option[ColumnMapping], columns: Seq[ColumnMapping]) extends SparkListener
  with DFOperation with DataFrameWriterExtensions with JdbcHelper {

  val mapping = TableMapping(idColumn, columns)

  private val jdbcOptions = new JDBCOptions(url, table, properties)

  private var initialRecords: Long = 0L

  override def preStart(sc: SparkContext): Unit = {
    initialRecords = getRowCount()
  }

  override def transform(df: DataFrame): DataFrame = {
    println(processedRows.value)
    ifNotEmpty(df) { df =>
      val ndf = mapping.targetDF(df)
      val idField = idColumn.map(id => StructField(id.target, id.`type`, nullable = false))
      ndf.write.mode(properties.get("savemode").getOrElse("append")).upsert(idField, jdbcOptions, ndf)
      println(processedRows.value)
      ndf
    }
  }

  override def validate: ValidationResult = {
    checkRequiredParams(Seq("url" -> url, "table" -> table, "user" -> properties.get("user").getOrElse(null)))
  }

  override val id: String = s"${ClassUtils.getShortCanonicalName(getClass)}_$table".toUpperCase

  private val connFact = JdbcUtils.createConnectionFactory(jdbcOptions)

  private def getRowCount(): Long = {
    withConnection(connFact()) { c =>
      try {
        val stmt = c.prepareStatement(s"select count(*) from $table")
        val rs = stmt.executeQuery()
        rs.next()
        val count = rs.getLong(1)
        stmt.close()
        count
      }
      catch {
        case e: SQLException =>
          log.warn(s"Unable to get row counts due to an exception: ${e.getMessage}")
          0L
      }
    }
  }

  override def onStageCompleted(s: SparkListenerStageCompleted): Unit = {
    if (s.stageInfo.name.startsWith("upsert")) {
      log.debug("Upsert finished")
      s.stageInfo.accumulables.find(_._2.name.exists(_.equalsIgnoreCase("number of output rows"))) match {
        case Some(acc) =>
          val finalRows = getRowCount()
          outputRows.add(acc._2.value.map(_.toString.toLong).getOrElse(0L))
          println(s"The table $table started with $initialRecords rows.")
          println(s"The $id operation processed ${processedRows.value} records.")
          println(s"The table $table ended with $finalRows rows.")
         if (idColumn.isDefined)
           println(s"Since this is an upsert, that means ${processedRows.value - finalRows} row(s) were updated.")
        case None => log.warn("No accumulable found for number of output rows")
      }
    }
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
    val table = cfg.get[String]("table").valueOrThrow(_ => new IllegalArgumentException("Table is required for DatabaseUpsert"))
    val url = cfg.get[String]("url").valueOrThrow(_ => new IllegalArgumentException("URL is required for DatabaseUpsert"))
    val idColumn = cfg.get[Config]("idColumn").map(v => mapping(v)).toOption
    val columnList = cfg.get[List[Config]]("columns").valueOrElse(Seq.empty)
    val columns = columnList.map(mapping)

    DatabaseUpsert(table, url, properties, idColumn, columns)
  }
}



