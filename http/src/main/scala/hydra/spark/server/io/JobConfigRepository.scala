package hydra.spark.server.io

import com.typesafe.config.Config
import hydra.spark.server.model.JobConfig
import hydra.spark.server.sql.{DatabaseComponent, JobConfigsComponent, ProfileComponent}
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

/**
  * Created by alexsilva on 6/2/17.
  */
trait JobConfigRepository {
  def saveJobConfig(jobId: String, jobConfig: Config): Future[JobConfig]

  def getJobConfig(jobId: String): Future[Option[com.typesafe.config.Config]]

}

class SlickJobConfigRepository()(implicit val db: Database, val profile: JdbcProfile)
  extends JobConfigRepository with JobConfigsComponent with DatabaseComponent with ProfileComponent {

  override def saveJobConfig(jobId: String, jobConfig: Config): Future[JobConfig] =
    JobConfigsRepository.create(JobConfig(None, jobId, jobConfig))

  override def getJobConfig(jobId: String): Future[Option[Config]] = JobConfigsRepository.findConfigByJobId(jobId)
}