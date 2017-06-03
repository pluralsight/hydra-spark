package hydra.spark.server.sql

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.server.model.JobConfig
import slick.lifted

import scala.concurrent.Future

trait JobConfigsComponent {

  this: DatabaseComponent with ProfileComponent =>

  import profile.api._
  import slick.lifted.Tag

  implicit val configMapper = profile.api.MappedColumnType.base[Config, String](
    e => e.root().render(), s => ConfigFactory.parseString(s))

  class JobConfigsTable(tag: Tag) extends Table[JobConfig](tag, "CONFIGS") {

    def jobId = column[String]("JOB_ID", O.PrimaryKey)

    def jobConfig = column[Config]("JOB_CONFIG")

    def * = (jobId, jobConfig) <> (JobConfig.tupled, JobConfig.unapply)
  }

  object JobConfigsRepository extends Repository[JobConfigsTable, String](profile, db) {

    val table = lifted.TableQuery[JobConfigsTable]

    val createQuery = table returning table.map(_.jobId) into ((item, id) => item.copy(jobId = id))

    def create(t: JobConfig): Future[JobConfig] = {
      val action = createQuery += t
      db.run(action)
    }

    def getId(table: JobConfigsTable) = table.jobId

    def findConfigByJobId(jobId: String): Future[Option[Config]] = {
      val query = table.filter(_.jobId === jobId).map(_.jobConfig).result
      db.run(query.headOption)
    }
  }

}
