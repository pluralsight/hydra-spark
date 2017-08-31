package hydra.spark.operations.jdbc

import java.sql.{Connection, ResultSet, Statement}

import scala.util.Try

trait JdbcHelper {

  private[jdbc] def connectionFactory: () => Connection

  private[this] def withResource[T <: AutoCloseable, R](resource: T)(code: (T) => R): R = {
    try {
      code(resource)
    } finally {
      resource.close()
    }
  }


  private[jdbc] def withConnection[T](f: (Connection) => T): T = withResource(connectionFactory())(c => f(c))


  private[jdbc] def withStatement[T](f: (Statement) => T): T = {
    withConnection { conn =>
      withResource(conn.createStatement())(f(_))
    }
  }

  private[jdbc] def withResultSet[T](selectSqlCmd: String)(f: (ResultSet) => T): T = {
    withStatement { s =>
      withResource(s.executeQuery(selectSqlCmd))(f(_))
    }
  }

  implicit class ResultSetStream(resultSet: ResultSet) {

    def toStream: Stream[ResultSet] = {
      new Iterator[ResultSet] {
        def hasNext = resultSet.next()

        def next() = resultSet
      }.toStream
    }
  }

  private[jdbc] def getRowCount(table: String): Try[Long] = {
    Try {
      withResultSet(s"select count(*) from $table") { rs =>
        rs.toStream.head.getLong(1)
      }
    }
  }
}
