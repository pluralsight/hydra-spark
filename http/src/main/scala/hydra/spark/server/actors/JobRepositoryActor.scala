package hydra.spark.server.actors


import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import hydra.spark.server.io._
import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 6/1/17.
  */
class JobRepositoryActor(jobRepo: JobRepository, binRepo: BinaryRepository, cfgRepo: JobConfigRepository) extends Actor {

  import JobRepositoryActor._
  import akka.pattern.pipe
  import context.dispatcher

  implicit val daoTimeout = 60 seconds

  def receive: Receive = {
    case SaveBinary(appName, binaryType, uploadTime, jarBytes) =>
      val future = binRepo.saveBinary(appName, binaryType, uploadTime, jarBytes)
      val s = sender
      future.map(i => SaveBinaryResult(Success(i))).recover { case e => SaveBinaryResult(Failure(e)) }.pipeTo(s)

    case DeleteBinary(appName) =>
      val future = binRepo.deleteBinary(appName)
      val s = sender
      future.map(DeleteBinaryResult(_)).recover { case _ => DeleteBinaryResult(false) }.pipeTo(s)

    case GetApps(typeFilter) =>
      val s = sender
      binRepo.getApps.map(apps => Apps(typeFilter.map(t => apps.filter(_._2._1 == t)).getOrElse(apps)))
        .pipeTo(s)

    case SaveJob(jobInfo) =>
      jobRepo.saveJobInfo(jobInfo)

    case GetJobs(limit) =>
      jobRepo.getJobs(limit).map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      cfgRepo.saveJobConfig(jobId, jobConfig)

    case GetJobConfig(jobId) =>
      cfgRepo.getJobConfig(jobId).map(JobConfig).pipeTo(sender)

    case GetLastUploadTimeAndType(appName) =>
      sender ! LastUploadTimeAndType(jobRepo.getLastUploadTimeAndType(appName))

    case GetBinaryContent(appName, binaryType, uploadTime) =>
      val fs = sender
      val response = binRepo.getBinaryContents(appName, binaryType, uploadTime)
      pipe(response) to fs
  }
}

object JobRepositoryActor {

  //Requests
  sealed trait JobDAORequest

  case class SaveBinary(appName: String,
                        binaryType: BinaryType,
                        uploadTime: DateTime,
                        jarBytes: Array[Byte]) extends JobDAORequest

  case class SaveBinaryResult(info: Try[BinaryInfo])

  case class DeleteBinary(appName: String) extends JobDAORequest

  case class DeleteBinaryResult(outcome: Boolean)

  case class GetApps(typeFilter: Option[BinaryType]) extends JobDAORequest


  case class GetBinaryContent(appName: String,
                              binaryType: BinaryType,
                              uploadTime: DateTime) extends JobDAORequest

  case class SaveJob(jobInfo: JobInfo) extends JobDAORequest

  case class GetJobs(limit: Int) extends JobDAORequest

  case class SaveJobConfig(jobId: String, jobConfig: Config) extends JobDAORequest

  case class GetJobConfig(jobId: String) extends JobDAORequest

  case class GetLastUploadTimeAndType(appName: String) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse

  case class Apps(apps: Map[String, (BinaryType, DateTime)]) extends JobDAOResponse

  case class BinaryPath(binPath: String) extends JobDAOResponse

  case class BinaryContent(content: Array[Byte]) extends JobDAOResponse

  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse

  case class JobConfigs(jobConfigs: Map[String, Config]) extends JobDAOResponse

  case class JobConfig(jobConfig: Option[Config]) extends JobDAOResponse

  case class LastUploadTimeAndType(uploadTimeAndType: Option[(DateTime, BinaryType)]) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse

  case object JarStored extends JobDAOResponse

  def props(repository: JobRepository): Props = Props(classOf[JobRepositoryActor], repository)
}