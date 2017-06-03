package hydra.spark.server.sql

import com.typesafe.config.Config
import configs.syntax._
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

/**
  * Created by alexsilva on 6/1/17.
  */
object FlywaySupport {

  private val flogger = LoggerFactory.getLogger(getClass)

  def migrate(config: Config) = {
    val urlConfig = config.get[String]("properties.url").toOption
    val userConfig = config.get[String]("properties.user").toOption
    val passwordConfig = config.get[String]("properties.password").toOption
    val migrateLocations = config.getString("flyway.locations")
    val initOnMigrate = config.get[Boolean]("flyway.initOnMigrate").valueOrElse(false)
    (urlConfig, userConfig, passwordConfig) match {
      case (Some(url), Some(user), Some(password)) =>
        val flyway = new Flyway()
        //todo: is there a way to get a datasource from slick?
        flyway.setDataSource(url, user, password)
        flyway.setLocations(migrateLocations)
        flyway.setBaselineOnMigrate(initOnMigrate)
        flyway.migrate()

      case _ => flogger.debug("Won't migrate database: Not configured properly; url, user and password are required.")
    }

  }
}
