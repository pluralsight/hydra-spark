package hydra.spark.server.actors

import akka.actor.ActorRef
import hydra.spark.server.io.FileInfo
import hydra.spark.server.model.Job
import org.joda.time.DateTime

/**
  * Created by alexsilva on 6/1/17.
  */
object Messages {

  case class Stored(info: FileInfo)

  case object Deleted

  case class Error(e: Throwable)

  case class Unsubscribe(jobId: String, receiver: ActorRef)

  case class Subscribe(jobId: String, receiver: ActorRef, events: Set[Class[_]]) {
    require(events.nonEmpty, "Must subscribe to at least one type of event!")
  }

  case class NoSuchJobId(msg: String)


  case object JobInitAlready

  trait JobStatusMessage {
    def jobId: String
  }

  case class JobStarted(jobId: String, jobInfo: Job) extends JobStatusMessage

  case class JobFinished(jobId: String, endTime: DateTime) extends JobStatusMessage

  case class InvalidJob(jobId: String, endTime: DateTime, err: Throwable) extends JobStatusMessage

  case class JobError(jobId: String, endTime: DateTime, err: Throwable) extends JobStatusMessage

  case class JobKilled(jobId: String, endTime: DateTime) extends JobStatusMessage


}
