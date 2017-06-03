package hydra.spark.server.io

/**
  * Created by alexsilva on 5/27/17.
  */

import java.io.{BufferedOutputStream, File, FileOutputStream, FilenameFilter}

import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

trait FileCacher {

  def rootDir: File

  private val logger = LoggerFactory.getLogger(getClass)

  def initFileDirectory(): Unit = {
    if (!rootDir.exists()) {
      if (!rootDir.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }
  }

  // date format
  val Pattern = "\\d{8}_\\d{6}_\\d{3}".r

  def createBinaryName(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    appName + "-" + uploadTime.toString("yyyyMMdd_hhmmss_SSS") + s".${binaryType.extension}"
  }

  /**
    * Caches the jar file into local file system.
    */
  protected def cacheBinary(appName: String, binaryType: BinaryType, uploadTime: DateTime, binBytes: Array[Byte]) {
    val outFile = new File(rootDir, createBinaryName(appName, binaryType, uploadTime))
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", binBytes.length, outFile.getPath)
      bos.write(binBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

  protected def cleanCacheBinaries(appName: String): Unit = {
    val binaries = rootDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        val prefix = appName + "-"
        if (name.startsWith(prefix)) {
          val suffix = name.substring(prefix.length)
          (Pattern findFirstIn suffix).isDefined
        }
        false
      }
    })
    if (binaries != null) {
      binaries.foreach(f => f.delete())
    }
  }

}