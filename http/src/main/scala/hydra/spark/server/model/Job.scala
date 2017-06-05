package hydra.spark.server.model

import hydra.spark.server.model.JobStatus.JobStatus
import org.joda.time.DateTime


case class Job(jobId: String, contextName: String, binId: Int, classPath: String, status: JobStatus,
               startTime: DateTime, endTime: Option[DateTime], error: Option[String])

object JobStatus extends Enumeration {
  type JobStatus = Value
  val Running, Error, Finished, Started, Killed, Unknown = Value
}
