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
    def configId = column[Int]("CFG_ID", O.PrimaryKey, O.AutoInc)

    def jobId = column[String]("JOB_ID")

    def jobConfig = column[Config]("JOB_CONFIG")

    def * = (configId.?, jobId, jobConfig) <> (JobConfig.tupled, JobConfig.unapply)
  }

  object JobConfigsRepository extends Repository[JobConfigsTable, Int](profile, db) {

    val table = lifted.TableQuery[JobConfigsTable]

    val createQuery = table returning table.map(_.configId) into ((item, id) => item.copy(configId = Some(id)))

    def create(t: JobConfig): Future[JobConfig] = {
      val action = createQuery += t
      db.run(action)
    }

    def getId(table: JobConfigsTable) = table.configId

    def findConfigByJobId(jobId: String): Future[Option[Config]] = {
      val query = table.filter(_.jobId === jobId).map(_.jobConfig).result
      db.run(query.headOption)
    }
  }

}
