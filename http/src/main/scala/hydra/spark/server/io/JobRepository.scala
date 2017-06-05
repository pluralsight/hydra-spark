package hydra.spark.server.io

import hydra.spark.server.job.BinaryType
import hydra.spark.server.model.Job
import hydra.spark.server.model.JobStatus.JobStatus
import hydra.spark.server.sql._
import org.joda.time.DateTime
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by alexsilva on 5/26/17.
  */
trait JobRepository {

  def getApps: Future[Map[String, (BinaryType, DateTime)]]

  def saveJobInfo(jobInfo: JobInfo): Future[JobInfo]

  def getJobInfo(jobId: String): Future[Option[JobInfo]]

  def getJobs(limit: Int, status: Option[JobStatus] = None): Future[Seq[JobInfo]]

  def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] =
    Await.result(getApps, 60 seconds).get(appName).map(t => (t._2, t._1))
}

class SlickJobRepository()(implicit val db: Database, val profile: JdbcProfile, ec: ExecutionContext)
  extends JobRepository with JobsComponent with DatabaseComponent with ProfileComponent {

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = BinariesRepository.getApps

  override def saveJobInfo(jobInfo: JobInfo): Future[JobInfo] = {

    val bin = BinariesRepository.findByArgs(
      jobInfo.binaryInfo.appName,
      jobInfo.binaryInfo.binaryType,
      jobInfo.binaryInfo.uploadTime)

    bin.flatMap { b =>
      val errors = jobInfo.error.map(e => e.getMessage)
      val job = Job(jobInfo.jobId, jobInfo.contextName, b.id.get, jobInfo.classPath,
        jobInfo.status, jobInfo.startTime, jobInfo.endTime, errors)
      JobsRepository.create(job).map { j =>
        JobInfo(job.jobId,
          job.contextName,
          BinaryInfo(b.appName, BinaryType.fromString(b.binaryType), b.uploadTime),
          job.classPath,
          job.startTime,
          job.endTime,
          job.status,
          job.error.map(new Throwable(_)))
      }

    }
  }

  override def getJobs(limit: Int, statusOpt: Option[JobStatus] = None): Future[Seq[JobInfo]] = {
    JobsRepository.getJobs(limit, statusOpt)
  }

  override def getJobInfo(jobId: String): Future[Option[JobInfo]] = {
    JobsRepository.getJobInfo(jobId)
  }
}

case class JobInfo(jobId: String, contextName: String,
                   binaryInfo: BinaryInfo, classPath: String,
                   startTime: DateTime, endTime: Option[DateTime],
                   status: JobStatus,
                   error: Option[Throwable]) {
  def jobLengthMillis: Option[Long] = endTime.map { end => new org.joda.time.Duration(startTime, end).getMillis }

  def isRunning: Boolean = endTime.isEmpty

  def isErroredOut: Boolean = endTime.isDefined && error.isDefined
}