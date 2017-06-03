package hydra.spark.server.sql

/**
  * Created by alexsilva on 2/9/17.
  */


import java.sql.Timestamp

import org.joda.time.DateTime
import slick.jdbc.JdbcProfile

trait ProfileComponent {
  val profile: JdbcProfile

  import profile.api._

  implicit lazy val dateTimeMapper = MappedColumnType.base[DateTime, Timestamp](
    e => new Timestamp(e.getMillis), s => new DateTime(s.getTime))
}