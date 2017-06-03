package hydra.spark.server.actors


import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.yammer.metrics.core.Meter
import hydra.spark.server.actors.JobManager.JobKilledException
import hydra.spark.server.io.JobInfo
import hydra.spark.server.util.DateUtils._
import org.joda.time.DateTime

import scala.collection.mutable
import scala.util.Try


/**
  * Created by alexsilva on 6/1/17.
  */

class JobStatusActor(jobDao: ActorRef) extends Actor with YammerMetrics {

  import JobStatusActor._
  import Messages._

  private val logger = Logging(this)

  private val infos = new mutable.HashMap[String, JobInfo]
  private val subscribers = new mutable.HashMap[String, mutable.MultiMap[Class[_], ActorRef]]
  private val metricNumSubscriptions = gauge("num-subscriptions", subscribers.size)
  private val metricNumJobs = gauge("num-running-jobs", infos.size)
  private val metricStatusRates = mutable.HashMap.empty[String, Meter]

  override def postStop(): Unit = {
    val stopTime = DateTime.now()
    infos.values.foreach { info =>
      val ninfo = info.copy(endTime = Some(stopTime),
        error = Some(new Exception(s"Context (${info.contextName}) for this job was terminated")))
      jobDao ! JobRepositoryActor.SaveJob(ninfo)
    }
  }

  override def receive: Receive = {
    case GetRunningJobStatus =>
      sender ! infos.values.toSeq.sortBy(_.startTime)

    case Unsubscribe(jobId, receiver) =>
      subscribers.get(jobId) match {
        case Some(jobSubscribers) =>
          jobSubscribers.transform { case (event, receivers) => receivers -= receiver }
            .retain { case (event, receivers) => receivers.nonEmpty }
          if (jobSubscribers.isEmpty) subscribers.remove(jobId)

        case None => sender ! NoSuchJobId(s"No registered subscribers for job $jobId (or no job with id $jobId).")
      }

    case Subscribe(jobId, receiver, events) =>
      val jobSubscribers = subscribers.getOrElseUpdate(jobId, newMultiMap())
      events.foreach { event => jobSubscribers.addBinding(event, receiver) }

    case JobInit(jobInfo) =>
      // TODO (kelvinchu): Check if the jobId exists in the persistence store already
      if (!infos.contains(jobInfo.jobId)) {
        infos(jobInfo.jobId) = jobInfo
      } else {
        sender ! JobInitAlready
      }

    case msg: JobStarted =>
      processStatus(msg, "started") {
        case (info, msg: JobStarted) => info.copy(startTime = msg.jobInfo.startTime)
      }

    case msg: JobFinished =>
      processStatus(msg, "finished OK", remove = true) {
        case (info, msg: JobFinished) =>
          info.copy(endTime = Some(msg.endTime))
      }

    case msg: InvalidJob =>
      processStatus(msg, "validation failed", remove = true) {
        case (info, msg: InvalidJob) =>
          info.copy(endTime = Some(msg.endTime), error = Some(msg.err))
      }

    case msg: JobError =>
      processStatus(msg, "finished with an error", remove = true) {
        case (info, msg: JobError) =>
          info.copy(endTime = Some(msg.endTime), error = Some(msg.err))
      }

    case msg: JobKilled =>
      processStatus(msg, "killed", remove = true) {
        case (info, msg: JobKilled) =>
          info.copy(endTime = Some(msg.endTime), error = Some(JobKilledException(info.jobId)))
      }
  }

  private def processStatus[M <: JobStatusMessage](msg: M, logMessage: String, remove: Boolean = false)
                                                  (infoModifier: (JobInfo, M) => JobInfo) {
    if (infos.contains(msg.jobId)) {
      infos(msg.jobId) = infoModifier(infos(msg.jobId), msg)
      logger.info("Job {} {}", msg.jobId: Any, logMessage)
      jobDao ! JobRepositoryActor.SaveJob(infos(msg.jobId))
      publishMessage(msg.jobId, msg)
      updateMessageRate(msg)
      if (remove) infos.remove(msg.jobId)
    } else {
      logger.error("No such job id " + msg.jobId)
      sender ! NoSuchJobId
    }
  }

  private def updateMessageRate(msg: JobStatusMessage) {
    val msgClass = msg.getClass.getCanonicalName

    lazy val getShortName = Try(msgClass.split('.').last).toOption.getOrElse(msgClass)

    metricStatusRates.getOrElseUpdate(msgClass, meter(getShortName, "messages")).mark()
  }

  private def publishMessage(jobId: String, message: JobStatusMessage) {
    for (
      jobSubscribers <- subscribers.get(jobId);
      receivers <- jobSubscribers.get(message.getClass);
      receiver <- receivers
    ) {
      receiver ! message
    }
  }

  private def newMultiMap(): mutable.MultiMap[Class[_], ActorRef] =
    new mutable.HashMap[Class[_], mutable.Set[ActorRef]] with mutable.MultiMap[Class[_], ActorRef]
}


object JobStatusActor {

  case class JobInit(jobInfo: JobInfo)

  case class GetRunningJobStatus()

  def props(jobDao: ActorRef): Props = Props(classOf[JobStatusActor], jobDao)
}