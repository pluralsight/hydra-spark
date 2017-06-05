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

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

/**
  * Created by alexsilva on 6/19/16.
  */

object DataFrameWriterExtensions {

  implicit class Upsert(w: DataFrameWriter[Row]) {
    def upsert(idColumn: Option[StructField], jdbcOptions: JDBCOptions, df: DataFrame): Unit = {
      val url = jdbcOptions.url
      val table = jdbcOptions.table
      val modeF = w.getClass.getDeclaredField("mode")
      modeF.setAccessible(true)
      val mode = modeF.get(w).asInstanceOf[SaveMode]
      val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()

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
          val schema = JdbcUtils.schemaString(df.schema, url)
          val dialect = JdbcDialects.get(url)
          val pk = idColumn.collect { case c: StructField => s", primary key(${dialect.quoteIdentifier(c.name)})" }
            .getOrElse("")
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
        case Some(id) => UpsertUtils.upsert(df, idColumn, jdbcOptions)
        case None => JdbcUtils.saveTable(df, url, table, jdbcOptions)
      }

    }

  }

}
