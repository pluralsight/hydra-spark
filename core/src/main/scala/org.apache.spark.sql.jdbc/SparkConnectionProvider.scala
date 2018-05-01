package org.apache.spark.sql.jdbc

import java.sql.Connection

import hydra.sql.ConnectionProvider
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

class SparkConnectionProvider(options: JDBCOptions) extends ConnectionProvider {

  private val connFact = JdbcUtils.createConnectionFactory(options)

  private lazy val conn = connFact()

  override def getConnection(): Connection = conn

  override def getNewConnection(): Connection = connFact()

  override def close(): Unit = conn.close()

  override def connectionUrl: String = options.url
}
