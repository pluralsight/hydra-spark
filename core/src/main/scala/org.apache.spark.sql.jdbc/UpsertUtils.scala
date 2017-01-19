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

import java.sql.{ Connection, PreparedStatement }
import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.util.control.NonFatal

/**
 * Created by alexsilva on 6/19/16.
 */
object UpsertUtils extends Logging {

  def upsert(df: DataFrame, idCol: Option[StructField], url: String, table: String, properties: Properties) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = JdbcUtils.createConnectionFactory(url, properties)
    val batchSize = properties.getProperty("batchsize", "1000").toInt
    df.foreachPartition { iterator =>
      upsertPartition(getConnection, table, iterator, idCol, rddSchema, nullTypes, batchSize, dialect)
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}")
    )
  }

  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def upsertPartition(
    getConnection: () => Connection,
    table: String,
    iterator: Iterator[Row],
    idColumn: Option[StructField],
    rddSchema: StructType,
    nullTypes: Array[Int],
    batchSize: Int,
    dialect: JdbcDialect
  ): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        log.warn("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val upsert = UpsertBuilder.forDriver(conn.getMetaData.getDriverName)
        .upsertStatement(conn, table, idColumn, rddSchema)

      val stmt = upsert.stmt
      val uschema = upsert.schema

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = uschema.fields.length
          uschema.fields.zipWithIndex.foreach {
            case (f, idx) =>
              val i = row.fieldIndex(f.name)
              if (row.isNullAt(i)) {
                stmt.setNull(idx, nullTypes(i))
              } else {
                uschema.fields(i).dataType match {
                  case IntegerType => stmt.setInt(idx + 1, row.getInt(i))
                  case LongType => stmt.setLong(idx + 1, row.getLong(i))
                  case DoubleType => stmt.setDouble(idx + 1, row.getDouble(i))
                  case FloatType => stmt.setFloat(idx + 1, row.getFloat(i))
                  case ShortType => stmt.setInt(idx + 1, row.getShort(i))
                  case ByteType => stmt.setInt(idx + 1, row.getByte(i))
                  case BooleanType => stmt.setBoolean(idx + 1, row.getBoolean(i))
                  case StringType => stmt.setString(idx + 1, row.getString(i))
                  case BinaryType => stmt.setBytes(idx + 1, row.getAs[Array[Byte]](i))
                  case TimestampType => stmt.setTimestamp(idx + 1, row.getAs[java.sql.Timestamp](i))
                  case DateType => stmt.setDate(idx + 1, row.getAs[java.sql.Date](i))
                  case t: DecimalType => stmt.setBigDecimal(idx + 1, row.getDecimal(i))
                  case ArrayType(et, _) =>
                    val array = conn.createArrayOf(
                      getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
                      row.getSeq[AnyRef](i).toArray
                    )
                    stmt.setArray(idx + 1, array)
                  case _ => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $i"
                  )
                }
              }

          }
          //          var i = 0
          //          while (i < numFields) {
          //            if (row.isNullAt(i)) {
          //              stmt.setNull(i + 1, nullTypes(i))
          //            } else {
          //              uschema.fields(i).dataType match {
          //                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
          //                case LongType => stmt.setLong(i + 1, row.getLong(i))
          //                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
          //                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
          //                case ShortType => stmt.setInt(i + 1, row.getShort(i))
          //                case ByteType => stmt.setInt(i + 1, row.getByte(i))
          //                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
          //                case StringType => stmt.setString(i + 1, row.getString(i))
          //                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
          //                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
          //                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
          //                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
          //                case ArrayType(et, _) =>
          //                  val array = conn.createArrayOf(
          //                    getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
          //                    row.getSeq[AnyRef](i).toArray)
          //                  stmt.setArray(i + 1, array)
          //                case _ => throw new IllegalArgumentException(
          //                  s"Can't translate non-null value for field $i")
          //              }
          //            }
          //            i = i + 1
          //          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => log.warn("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }
}

trait UpsertBuilder {

  def upsertStatement(conn: Connection, table: String, id: Option[StructField], schema: StructType): UpsertInfo
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

object PostgresUpsertBuilder extends UpsertBuilder {
  def upsertStatement(conn: Connection, table: String, idField: Option[StructField], schema: StructType) = {
    idField match {
      case Some(id) => {
        val cSchema = StructType(schema.fields.filterNot(_.name == id.name))
        val columns = cSchema.fields.map(_.name).mkString(",")
        val placeholders = cSchema.fields.map(_ => "?").mkString(",")
        val sql =
          s"""insert into ${table} (${id.name}, $columns) values (?, $placeholders)
              |on conflict (${id.name})
              |do update set ($columns) = ($placeholders)
              |where ${table}.${id.name} = ?;""".stripMargin

        val schemaFields = Seq(id) ++ cSchema.fields ++ cSchema.fields :+ id
        val upsertSchema = StructType(schemaFields)
        UpsertInfo(conn.prepareStatement(sql), upsertSchema)
      }
      case None => {
        UpsertInfo(JdbcUtils.insertStatement(conn, table, schema), schema)
      }
    }
  }
}

object H2UpsertBuilder extends UpsertBuilder {
  def upsertStatement(conn: Connection, table: String, idField: Option[StructField], schema: StructType) = {
    idField match {
      case Some(id) => {
        val cSchema = StructType(schema.fields.filterNot(_.name == id.name))
        val columns = cSchema.fields.map(_.name).mkString(",")
        val placeholders = cSchema.fields.map(_ => "?").mkString(",")
        val sql =
          s"""merge into ${table} (${id.name}, $columns) key(${id.name}) values (?, $placeholders);"""
            .stripMargin
        //H2 is nice enough to keep the same parameter list
        UpsertInfo(conn.prepareStatement(sql), schema)
      }
      case None => {
        UpsertInfo(JdbcUtils.insertStatement(conn, table, schema), schema)
      }
    }
  }
}
