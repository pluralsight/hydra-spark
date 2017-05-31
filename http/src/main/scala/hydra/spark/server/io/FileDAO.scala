package hydra.spark.server.io

/**
  * Created by alexsilva on 5/26/17.
  */

import java.io._
import java.nio.file.{FileAlreadyExistsException, Files}
import java.util.NoSuchElementException

import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Try}


case class FileDAO(rootDirPath: String) {

  import FileDAO._

  private val files = mutable.HashMap.empty[String, FileInfo]

  private val dataFile: File = {
    val rootDirFile = new File(rootDirPath)
    logger.trace(s"rootDir is ${rootDirFile.getAbsolutePath}")
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException(s"Could not create directory $rootDirPath")
      }
    }

    val dataFile = new File(rootDirFile, META_DATA_FILE_NAME)
    // read back all files info during startup
    if (dataFile.exists()) {
      val in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)))
      try {
        while (true) {
          val dataInfo = readFileInfo(dataFile.getAbsolutePath, in)
          addFile(dataInfo)
        }
      } catch {
        case e: EOFException => // do nothing
      } finally {
        in.close()
      }
    }
    dataFile
  }

  val rootDir = dataFile.getParentFile.getAbsolutePath


  private val dataOutputStream = new DataOutputStream(new FileOutputStream(dataFile, true))

  def shutdown(): Unit = {
    Try(dataOutputStream.close()).recover { case t => logger.error(s"unable to close output stream:${t.getMessage}") }
  }


  def saveFile(appName: String, uploadTime: DateTime, bt: BinaryType, aBytes: Array[Byte]): Try[FileInfo] = {
    Try {
      if (files.contains(appName))
        throw new FileAlreadyExistsException(s"File $appName already exists.")
      val outFile = new File(rootDirPath, createFileName(appName, bt, uploadTime))
      new BufferedOutputStream(new FileOutputStream(outFile)) -> outFile
    }.map { case (bos, outFile) =>
      // The order is important. Save the file first and then log it into meta data file.
      logger.debug("Writing {} bytes to file {}", aBytes.length, outFile.getPath)
      bos.write(aBytes)
      bos.flush()
      bos.close()
      // log it into meta data file
      val info = FileInfo(appName, bt, outFile.getPath, uploadTime)
      writeFileInfo(dataOutputStream, info)
      addFile(info)
      info
    }
  }

  private def addFile(info: FileInfo) = files.put(info.appName, info)

  private def writeFileInfo(out: DataOutputStream, aInfo: FileInfo) {
    out.writeUTF(aInfo.appName)
    out.writeUTF(aInfo.btype.name)
    out.writeLong(aInfo.uploadTime.getMillis)
  }

  def deleteFile(aName: String): Try[Boolean] = {
    Try(files(aName)).map { info =>
      Files.delete(new File(info.path).toPath)
      files.remove(aName)
      true
    }.recoverWith { case _: NoSuchElementException =>
      Failure(new IllegalArgumentException(s"No Application $aName found."))
    }
  }

  private def readFileInfo(path: String, in: DataInputStream) =
    FileInfo(in.readUTF, BinaryType.fromString(in.readUTF()), path, new DateTime(in.readLong))

  def listFiles(ftype: Option[BinaryType]): Seq[FileInfo] = {
    ftype.map(bt => files.values.filter(p => p.btype == bt)).getOrElse(files.values).toSeq
  }

  private def createFileName(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    appName + "-" + uploadTime.toString("yyyyMMdd_hhmmss_SSS") + s".${binaryType.extension}"
  }

}

object FileDAO {
  private val logger = LoggerFactory.getLogger(getClass)
  //  val EXTENSION = ".dat"
  val META_DATA_FILE_NAME = "hydra-spark.data"
}