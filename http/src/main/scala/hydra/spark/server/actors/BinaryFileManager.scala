package hydra.spark.server.actors

import akka.actor.Actor
import akka.event.Logging
import akka.util.Timeout
import hydra.spark.server.actors.BinaryFileManager._
import hydra.spark.server.io.{FileDAO, FileInfo}
import hydra.spark.server.job.BinaryType
import hydra.spark.server.util.JarUtils
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  *
  * Created by alexsilva on 5/31/17.
  */
class BinaryFileManager(fileDAO: FileDAO) extends Actor {

  implicit val daoAskTimeout = Timeout(60 seconds)

  private val logger = Logging(this)

  override def receive: Receive = {
    case ListBinaries(filterOpt) =>
      sender ! fileDAO.listFiles(filterOpt)

    case StoreBinary(appName, binaryType, binBytes) =>
      logger.info(s"Storing binary of type ${binaryType.name} for app $appName, ${binBytes.length} bytes")
      val result = if (!JarUtils.binaryIsZip(binBytes)) InvalidBinary
      else {
        fileDAO.saveFile(appName, DateTime.now(), binaryType, binBytes) match {
          case Success(x) => BinaryStored(x)
          case Failure(ex) => Error(ex)
        }
      }

      sender ! result

    case DeleteBinary(appName) =>
      logger.info(s"Deleting binary $appName")
      val result = fileDAO.deleteFile(appName) match {
        case Success(_) => BinaryDeleted
        case Failure(ex) => Error(ex)
      }
      sender ! result
  }
}

object BinaryFileManager {

  case class StoreBinary(appName: String, binaryType: BinaryType, binBytes: Array[Byte])

  case class DeleteBinary(appName: String)

  case class ListBinaries(typeFilter: Option[BinaryType])

  case object InvalidBinary

  case class BinaryStored(info: FileInfo)

  case object BinaryDeleted

  case class Error(ex: Throwable)

}