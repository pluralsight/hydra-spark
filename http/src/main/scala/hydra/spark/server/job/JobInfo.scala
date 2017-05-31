package hydra.spark.server.job


import org.joda.time.{DateTime, Duration}

/**
  * Created by alexsilva on 5/26/17.
  */
case class JobInfo(jobId: String, contextName: String,
                   binaryInfo: BinaryInfo, classPath: String,
                   startTime: DateTime, endTime: Option[DateTime],
                   error: Option[Throwable]) {
  def jobLengthMillis: Option[Long] = endTime.map(end => new Duration(startTime, end).getMillis)

  def isRunning: Boolean = endTime.isEmpty

  def isErroredOut: Boolean = endTime.isDefined && error.isDefined
}


object JobStatus extends Enumeration {
  type JobStatus = Value
  val Running, Error, Finished, Started, Killed, Unknown = Value
}


case class BinaryInfo(appName: String, binaryType: BinaryType, uploadTime: DateTime)