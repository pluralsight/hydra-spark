package hydra.spark.server.actors

import java.nio.file.{Files, Paths}

import akka.actor.{Actor, Props}
import akka.event.Logging
import akka.util.Timeout
import hydra.spark.server.actors.BinaryFileManager._
import hydra.spark.server.actors.Messages._
import hydra.spark.server.io.FileDAO
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

    case StoreBinary(appName, binaryType, path) =>
      val binBytes = Files.readAllBytes(Paths.get(path))
      logger.info(s"Storing binary of type ${binaryType.name} for app $appName, ${binBytes.length} bytes")
      val result = if (!JarUtils.binaryIsZip(binBytes)) {
        Error(new IllegalArgumentException(s"File $appName is not a valid ${binaryType.name.toLowerCase} file."))
      }
      else {
        fileDAO.saveFile(appName, DateTime.now(), binaryType, binBytes) match {
          case Success(x) => Stored(x)
          case Failure(ex) => Error(ex)
        }
      }

      sender ! result

    case DeleteBinary(appName) =>
      logger.info(s"Deleting binary $appName")
      val result = fileDAO.deleteFile(appName) match {
        case Success(_) => Deleted
        case Failure(ex) => Error(ex)
      }
      sender ! result
  }
}

object BinaryFileManager {

  case class StoreBinary(appName: String, binaryType: BinaryType, path: String)

  case class DeleteBinary(appName: String)

  case class ListBinaries(typeFilter: Option[BinaryType])

  def props(fileDAO: FileDAO): Props = Props(classOf[BinaryFileManager], fileDAO)

}