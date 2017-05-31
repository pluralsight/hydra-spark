package hydra.spark.server.io

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}

import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/26/17.
  */
class FileDAOSpec extends Matchers with FunSpecLike with BeforeAndAfterAll with BeforeAndAfterEach {

  val tmpFolder = Files.createTempDirectory("hs")

  val bytes = Array[Byte](0, 1, 2, 3, 4)

  val fileDao = new FileDAO(tmpFolder.toString)

  override def beforeEach() = {
    fileDao.deleteFile("hydra-spark")
  }

  describe("The file DAO") {
    it("is configured") {
      fileDao.rootDirPath shouldBe tmpFolder.toString
    }

    it("saves") {
      val dt = DateTime.now
      val expectedPath = s"${tmpFolder.toString}/hydra-spark-${dt.toString().replace(':', '_')}.dat"
      val info = fileDao.saveFile("hydra-spark", dt,BinaryType.Data,  bytes)
      info shouldBe FileInfo("hydra-spark", BinaryType.Data, expectedPath, _: DateTime)
    }

    it("deletes") {
      val path = fileDao.saveFile("hydra-spark", DateTime.now,BinaryType.Data,  bytes)
      fileDao.deleteFile(path.get.appName).get shouldBe true
      fileDao.deleteFile("some-file").isFailure shouldBe true
    }

    it("lists") {
      val path = fileDao.saveFile("hydra-spark", DateTime.now, BinaryType.Data, bytes)
      fileDao.listFiles(Some(BinaryType.Data)).exists(f => f.appName == "hydra-spark") shouldBe true
      fileDao.listFiles(None).exists(f => f.appName == "hydra-spark") shouldBe true
    }

  }

  override def afterAll() = {
    Files.walkFileTree(tmpFolder, new FileVisitor[Path] {
      val c = FileVisitResult.CONTINUE

      override def postVisitDirectory(dir: Path, exc: IOException) = c

      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        Files.delete(file)
        c
      }

      override def visitFileFailed(file: Path, exc: IOException) = c

      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = c
    })
    Files.delete(tmpFolder)
    fileDao.shutdown()
  }

}
