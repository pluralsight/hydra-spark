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

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._

/**
  * Created by alexsilva on 6/19/16.
  */


trait UpsertBuilder {

  def upsertStatement(conn: Connection, table: String, dialect: JdbcDialect, idField: Option[StructField],
                      schema: StructType): UpsertInfo
}

/**
  *
  * @param stmt
  * @param schema The modified schema.  Postgres upserts, for instance, add fields to the SQL, so we update the
  *               schema to reflect that.
  */
case class UpsertInfo(stmt: PreparedStatement, schema: StructType)

object UpsertBuilder {
  val b = Map("postgres" -> PostgresUpsertBuilder, "h2" -> H2UpsertBuilder)

  def forDriver(driver: String): UpsertBuilder = {
    val builder = b.filterKeys(k => driver.toLowerCase().contains(k.toLowerCase()))
    require(builder.size == 1, "No upsert dialect registered for " + driver)
    builder.head._2
  }

}

object PostgresUpsertBuilder extends UpsertBuilder with Logging {
  def upsertStatement(conn: Connection, table: String, dialect: JdbcDialect, idField: Option[StructField],
                      schema: StructType) = {
    idField match {
      case Some(id) => {
        val columns = schema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val placeholders = schema.fields.map(_ => "?").mkString(",")
        val updateSchema = StructType(schema.fields.filterNot(_.name == id.name))
        val updateColumns = updateSchema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(",")
        val updatePlaceholders = updateSchema.fields.map(_ => "?").mkString(",")
        val sql =
          s"""insert into ${table} ($columns) values ($placeholders)
             |on conflict (${dialect.quoteIdentifier(id.name)})
             |do update set ($updateColumns) = ($updatePlaceholders)
             |where ${table}.${id.name} = ?;""".stripMargin

        log.debug(s"Using sql $sql")

        val schemaFields = schema.fields ++ updateSchema.fields :+ id
        val upsertSchema = StructType(schemaFields)
        UpsertInfo(conn.prepareStatement(sql), upsertSchema)
      }
      case None => {
        UpsertInfo(JdbcUtils.insertStatement(conn, table, schema, dialect), schema)
      }
    }
  }
}

object H2UpsertBuilder extends UpsertBuilder {
  def upsertStatement(conn: Connection, table: String, dialect: JdbcDialect, idField: Option[StructField],
                      schema: StructType) = {
    idField match {
      case Some(id) => {
        val columns = schema.fields.map(c => dialect.quoteIdentifier(c.name)).mkString(",")
        val placeholders = schema.fields.map(_ => "?").mkString(",")
        val pk = dialect.quoteIdentifier(id.name)
        val sql =
          s"""merge into ${table} ($columns) key($pk) values ($placeholders);"""
            .stripMargin
        //H2 is nice enough to keep the same parameter list
        UpsertInfo(conn.prepareStatement(sql), schema)
      }
      case None => {
        UpsertInfo(JdbcUtils.insertStatement(conn, table, schema, dialect), schema)
      }
    }
  }
}
