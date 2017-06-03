package hydra.spark.server.actors


import java.nio.file.{Files, Paths}

import akka.actor.{Actor, Props}
import hydra.spark.server.actors.Messages._
import hydra.spark.server.io.FileDAO
import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime

import scala.util.{Failure, Success}

/**
  * Created by alexsilva on 5/26/17.
  */

class DataFileManager(fileDao: FileDAO) extends Actor {

  import DataFileManager._

  override def receive: Receive = {
    case ListDataFiles => sender ! fileDao.listFiles(Some(BinaryType.Data))

    case DeleteDataFile(fileName) =>

      val result = fileDao.deleteFile(fileName) match {
        case Success(_) => Deleted
        case Failure(ex) => Error(ex)
      }
      sender ! result

    case StoreDataFile(appName, path) =>
      val result = fileDao.saveFile(appName, DateTime.now, BinaryType.Data, Files.readAllBytes(Paths.get(path))) match {
        case Success(x) => Stored(x)
        case Failure(ex) => Error(ex)
      }
      sender ! result
  }
}

object DataFileManager {

  case class StoreDataFile(appName: String, path: String)

  case class DeleteDataFile(name: String)

  case object ListDataFiles

  def props(fileDAO: FileDAO): Props = Props(classOf[DataFileManager], fileDAO)

}
