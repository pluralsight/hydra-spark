package hydra.spark.server.dao

import hydra.spark.server.io.{BinaryInfo, JobInfo}
import hydra.spark.server.job.BinaryType
import hydra.spark.server.model.JobStatus
import hydra.spark.server.model.JobStatus.JobStatus
import org.joda.time.DateTime

/**
  * Created by alexsilva on 6/3/17.
  */
trait DAOSpecBase {
  val time: DateTime = new DateTime()

  val throwable: Throwable = new Throwable("test-error")
  val jarInfo: BinaryInfo = genJarInfo(false, false)
  val jobInfoNoEndNoErr: JobInfo = genJobInfo(jarInfo, false, false, JobStatus.Running, false)
  val jobId: String = jobInfoNoEndNoErr.jobId

  def genJarInfo: (Boolean, Boolean) => BinaryInfo = genJarInfoClosure

  private def genJarInfoClosure = {
    var appCount: Int = 0
    var timeCount: Int = 0

    def genTestJarInfo(newAppName: Boolean, newTime: Boolean): BinaryInfo = {
      appCount = appCount + (if (newAppName) 1 else 0)
      timeCount = timeCount + (if (newTime) 1 else 0)

      val app = "test-appName" + appCount
      val upload = if (newTime) time.plusMinutes(timeCount) else time

      BinaryInfo(app, BinaryType.Jar, upload)
    }

    genTestJarInfo _
  }

  private def genJobInfoClosure = {
    var count: Int = 0

    def genTestJobInfo(jarInfo: BinaryInfo,
                       hasEndTime: Boolean,
                       hasError: Boolean,
                       status: JobStatus,
                       isNew: Boolean): JobInfo = {
      count = count + (if (isNew) 1 else 0)
      val id: String = "test-id" + count
      val contextName: String = "test-context"
      val classPath: String = "test-classpath"
      val startTime: DateTime = time

      val noEndTime: Option[DateTime] = None
      val someEndTime: Option[DateTime] = Some(time) // Any DateTime Option is fine
      val noError: Option[Throwable] = None
      val someError: Option[Throwable] = Some(throwable)

      val endTime: Option[DateTime] = if (hasEndTime) someEndTime else noEndTime
      val error: Option[Throwable] = if (hasError) someError else noError

      JobInfo(id, contextName, jarInfo, classPath, startTime, endTime, status, error)
    }

    genTestJobInfo _
  }

  def genJobInfo: (BinaryInfo, Boolean, Boolean, JobStatus, Boolean) => JobInfo = genJobInfoClosure
}
