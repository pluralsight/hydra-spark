package org.apache.spark.sql.jdbc

import java.sql.{Connection, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{DataType, HIVE_TYPE_STRING, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

import scala.util.control.NonFatal

object HydraJdbcUtils extends Logging {

  def createTable(
                   conn: Connection,
                   schema: StructType, sparkSession: SparkSession,
                   options: JDBCOptions): Unit = {
    val strSchema = schemaString(schema, sparkSession, options.url, options.createTableColumnTypes)
    val table = options.table
    val createTableOptions = options.createTableOptions
    // Create the table if the table does not exist.
    // To allow certain options to append when create a new table, which can be
    // table_options or partition_options.
    // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  def schemaString(schema: StructType, sparkSession: SparkSession,
                   url: String,
                   createTableColumnTypes: Option[String] = None): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    val userSpecifiedColTypesMap = createTableColumnTypes
      .map(parseUserSpecifiedCreateTableColumnTypes(schema, sparkSession, _))
      .getOrElse(Map.empty[String, String])
    schema.fields.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = userSpecifiedColTypesMap
        .getOrElse(field.name, getJdbcType(field.dataType, dialect).databaseTypeDefinition)
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * Parses the user specified createTableColumnTypes option value string specified in the same
    * format as create table ddl column types, and returns Map of field name and the data type to
    * use in-place of the default data type.
    */
  private def parseUserSpecifiedCreateTableColumnTypes(
                                                        schema: StructType, sparkSession: SparkSession,
                                                        createTableColumnTypes: String): Map[String, String] = {
    def typeName(f: StructField): String = {
      // char/varchar gets translated to string type. Real data type specified by the user
      // is available in the field metadata as HIVE_TYPE_STRING
      if (f.metadata.contains(HIVE_TYPE_STRING)) {
        f.metadata.getString(HIVE_TYPE_STRING)
      } else {
        f.dataType.catalogString
      }
    }

    val userSchema = CatalystSqlParser.parseTableSchema(createTableColumnTypes)
    val resolver = sparkSession.sessionState.conf.resolver

    // checks duplicate columns in the user specified column types.
    SchemaUtils.checkColumnNameDuplication(
      userSchema.map(_.name), "in the createTableColumnTypes option value", resolver)

    // checks if user specified column names exist in the DataFrame schema
    userSchema.fieldNames.foreach { col =>
      schema.find(f => resolver(f.name, col)).getOrElse {
        throw new AnalysisException(
          s"createTableColumnTypes option column $col not found in schema " +
            schema.catalogString)
      }
    }

    val userSchemaMap = userSchema.fields.map(f => f.name -> typeName(f)).toMap
    val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    if (isCaseSensitive) userSchemaMap else CaseInsensitiveMap(userSchemaMap)
  }

  def saveInternalPartition(
                             getConnection: () => Connection,
                             table: String,
                             iterator: Iterator[InternalRow],
                             rddSchema: StructType,
                             insertStmt: String,
                             batchSize: Int,
                             dialect: JdbcDialect,
                             isolationLevel: Int): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)
      //val setters = rddSchema.fields.map(f => makeInternalSetter(conn, dialect, f.dataType))
      // val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              // stmt.setNull(i + 1, nullTypes(i))
            } else {
              //      setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
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
      Iterator.empty
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          if (e.getCause == null) {
            e.initCause(cause)
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
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
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }
}
