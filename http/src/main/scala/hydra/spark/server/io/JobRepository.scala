package hydra.spark.server.io

import com.typesafe.config.Config
import hydra.spark.server.job.{BinaryType, JobInfo}
import hydra.spark.server.job.JobStatus.JobStatus
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by alexsilva on 5/26/17.
  */
trait JobRepository {
  /**
    * Persist a jar.
    *
    * @param appName
    * @param uploadTime
    * @param binaryBytes
    */
  def saveBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime, binaryBytes: Array[Byte])

  /**
    * Delete a jar.
    *
    * @param appName
    */
  def deleteBinary(appName: String)

  /**
    * Return all applications name and their last upload times.
    *
    * @return
    */
  def getApps: Future[Map[String, (BinaryType, DateTime)]]


  /**
    * Persist a job info.
    *
    * @param jobInfo
    */
  def saveJobInfo(jobInfo: JobInfo)

  /**
    * Return job info for a specific job id.
    *
    * @return
    */
  def getJobInfo(jobId: String): Future[Option[JobInfo]]

  /**
    * Return all job ids to their job info.
    *
    * @return
    */
  def getJobInfos(limit: Int, status: Option[JobStatus] = None): Future[Seq[JobInfo]]

  /**
    * Persist a job configuration along with provided jobId.
    *
    * @param jobId
    * @param jobConfig
    */
  def saveJobConfig(jobId: String, jobConfig: Config)


  /**
    * Returns a config for a given jobId
    *
    * @return
    */
  def getJobConfig(jobId: String): Future[Option[Config]]

  /**
    * Returns the last upload time for a given app name.
    *
    * @return Some(lastUploadedTime) if the app exists and the list of times is nonempty, None otherwise
    */
  def getLastUploadTimeAndType(appName: String): Option[(DateTime, BinaryType)] =
    Await.result(getApps, 60 seconds).get(appName).map(t => (t._2, t._1))

  /**
    * Fetch submited jar or egg content for remote driver and JobManagerActor to cache in local
    *
    * @param appName
    * @param uploadTime
    * @return
    */
  def getBinaryContent(appName: String,
                       binaryType: BinaryType,
                       uploadTime: DateTime): Array[Byte]
}