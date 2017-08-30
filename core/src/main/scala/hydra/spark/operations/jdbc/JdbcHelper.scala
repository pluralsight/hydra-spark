package hydra.spark.operations.jdbc

import java.sql.Connection

trait JdbcHelper {

  private[jdbc] def withConnection[T](conn: => Connection)(body: Connection => T): T = synchronized {
    val r = conn
    try {
      body(r)
    }
    finally {
      r.close()
    }
  }
}
