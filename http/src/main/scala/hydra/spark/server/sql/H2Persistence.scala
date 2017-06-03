package hydra.spark.server.sql

import slick.jdbc.JdbcBackend.Database

/**
  * Created by alexsilva on 2/9/17.
  */
trait H2Persistence extends ProfileComponent with DatabaseComponent {

  val h2Db = Database.forConfig("h2-db")

  val h2Profile = slick.jdbc.H2Profile

  implicit val profile = h2Profile
  implicit val db: Database = h2Db
}
