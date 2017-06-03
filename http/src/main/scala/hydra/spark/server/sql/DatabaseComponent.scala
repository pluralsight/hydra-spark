package hydra.spark.server.sql

/**
  * Created by alexsilva on 2/9/17.
  */

import slick.jdbc.JdbcBackend.Database

trait DatabaseComponent {
  val db: Database
}