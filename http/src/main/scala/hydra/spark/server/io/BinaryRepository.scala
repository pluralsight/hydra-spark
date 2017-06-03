package hydra.spark.server.io

import java.io.File

import hydra.spark.server.job.BinaryType
import hydra.spark.server.sql.{BinariesComponent, DatabaseComponent, ProfileComponent}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 6/2/17.
  */
trait BinaryRepository extends FileCacher {

  def saveBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime,
                 binBytes: Array[Byte]): Future[BinaryInfo]

  def deleteBinary(appName: String): Future[Boolean]

  def getBinaryInfo(appName: String,
                    binaryType: BinaryType,
                    uploadTime: DateTime): Future[BinaryInfo]

  def getBinaryContents(appName: String,
                        binaryType: BinaryType,
                        uploadTime: DateTime): Future[Array[Byte]]

  def getBinaryPath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String

  def getApps: Future[Map[String, (BinaryType, DateTime)]]

}

case class BinaryInfo(appName: String, binaryType: BinaryType, uploadTime: DateTime)

class SlickBinaryRepository(rootDirPath: String)
                           (implicit val db: Database, val profile: JdbcProfile, ec: ExecutionContext)
  extends BinaryRepository with BinariesComponent with DatabaseComponent with ProfileComponent {

  private val logger = LoggerFactory.getLogger(getClass)

  override val rootDir = new File(rootDirPath)

  logger.info("rootDir is " + rootDir)

  initFileDirectory()

  override def saveBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime,
                          binBytes: Array[Byte]): Future[BinaryInfo] = {
    cacheBinary(appName, binaryType, uploadTime, binBytes)
    BinariesRepository.create(Binary(None, appName, binaryType.name, uploadTime, binBytes))
      .map(bin => BinaryInfo(bin.appName, BinaryType.fromString(bin.binaryType), bin.uploadTime))
  }

  override def deleteBinary(appName: String): Future[Boolean] = {
    BinariesRepository.deleteByAppName(appName).map(_ == 1)
  }

  override def getBinaryInfo(appName: String, binaryType: BinaryType, uploadTime: DateTime): Future[BinaryInfo] = {
    BinariesRepository.fetchBinary(appName, binaryType, uploadTime)
      .map(bin => BinaryInfo(bin.appName, BinaryType.fromString(bin.binaryType), bin.uploadTime))
  }

  override def getBinaryContents(appName: String,
                                 binaryType: BinaryType, uploadTime: DateTime): Future[Array[Byte]] = {
    BinariesRepository.fetchBinary(appName, binaryType, uploadTime).map { bin =>
      cacheBinary(appName, binaryType, uploadTime, bin.binary)
      bin.binary
    }
  }

  override def getBinaryPath(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    val binFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    if (!binFile.exists()) {
      fetchAndCacheBinFile(appName, binaryType, uploadTime)
    }
    binFile.getAbsolutePath
  }

  override def getApps: Future[Map[String, (BinaryType, DateTime)]] = {
    BinariesRepository.getApps()
  }

  private def fetchAndCacheBinFile(appName: String, bType: BinaryType, uploadTime: DateTime): Future[BinaryInfo] = {
    BinariesRepository.fetchBinary(appName, bType, uploadTime).map { bin =>
      cacheBinary(appName, bType, uploadTime, bin.binary)
      BinaryInfo(bin.appName, BinaryType.fromString(bin.binaryType), bin.uploadTime)
    }
  }
}