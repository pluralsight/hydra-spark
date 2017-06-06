package hydra.spark.server.common.actors

/**
  * Created by alexsilva on 6/6/17.
  */

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// Taken from http://letitcrash.com/post/30165507578/shutdown-patterns-in-akka-2

object Reaper {

  // Used by others to register an Actor for watching
  case class WatchMe(ref: ActorRef)

  case object Reaped

}

abstract class Reaper extends Actor with ActorLogging {

  import Reaper._

  // Keep track of what we're watching
  val watched = ArrayBuffer.empty[ActorRef]

  def allSoulsReaped(): Unit

  // Watch and check for termination
  override def receive: Receive = {
    case Reaped =>
      watched.isEmpty

    case WatchMe(ref) =>
      log.info("Watching actor {}", ref)
      context.watch(ref)
      watched += ref

    case Terminated(ref) =>
      log.info("Actor {} terminated", ref)
      watched -= ref
      if (watched.isEmpty) allSoulsReaped()
  }
}

class ProductionReaper extends Reaper {
  def allSoulsReaped(): Unit = {
    log.warning("Shutting down actor system because all actors have terminated")
    Await.result(context.system.whenTerminated, Duration.Inf)
  }
}
