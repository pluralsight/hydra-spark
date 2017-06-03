package hydra.spark.server.sql

import slick.jdbc.JdbcBackend.Database

/**
  * Created by alexsilva on 2/9/17.
  */
trait PgPersistence extends ProfileComponent with DatabaseComponent {

  val pgDb = Database.forConfig("pg-db")

  val postgresProfile = slick.jdbc.PostgresProfile

  implicit val profile = postgresProfile
  implicit val db: Database = pgDb
}
