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

package org.apache.spark.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{ DataFrame, DataFrameWriter, SaveMode }

/**
 * Created by alexsilva on 6/19/16.
 */

object DataFrameWriterExtensions {

  implicit class Upsert(w: DataFrameWriter) {
    def upsert(url: String, table: String, idColumn: Option[StructField], connectionProperties: Properties,
      df: DataFrame): Unit = {

      val props = new Properties()

      val modeF = w.getClass.getDeclaredField("mode")
      modeF.setAccessible(true)
      val mode = modeF.get(w).asInstanceOf[SaveMode]

      props.putAll(connectionProperties)
      val conn = JdbcUtils.createConnectionFactory(url, props)()

      try {
        var tableExists = JdbcUtils.tableExists(conn, url, table)

        if (mode == SaveMode.Ignore && tableExists) {
          return
        }

        if (mode == SaveMode.ErrorIfExists && tableExists) {
          sys.error(s"Table $table already exists.")
        }

        if (mode == SaveMode.Overwrite && tableExists) {
          JdbcUtils.dropTable(conn, table)
          tableExists = false
        }

        // Create the table if the table didn't exist.
        if (!tableExists) {
          val schema = JdbcUtils.schemaString(df, url)
          val pk = idColumn.map(id => id.name) match {
            case Some(s: String) => s", primary key($s)"
            case _ => ""
          }
          val sql = s"CREATE TABLE $table ( $schema $pk )"
          val statement = conn.createStatement
          try {
            statement.executeUpdate(sql)
          } finally {
            statement.close()
          }
        }
      } finally {
        conn.close()
      }

      //todo: make this a single method
      idColumn match {
        case Some(id) => UpsertUtils.upsert(df, idColumn, url, table, props)
        case None => JdbcUtils.saveTable(df, url, table, props)
      }

    }

  }

}
