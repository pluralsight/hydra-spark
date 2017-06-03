package hydra.spark.server.actors

import java.nio.file.{FileAlreadyExistsException, Files}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import hydra.spark.server.actors.BinaryFileManager.{DeleteBinary, ListBinaries, StoreBinary}
import hydra.spark.server.actors.Messages._
import hydra.spark.server.io.FileDAO
import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by alexsilva on 5/31/17.
  */
class BinaryFileManagerSpec extends TestKit(ActorSystem("hydra-spark")) with WordSpecLike with Matchers
  with ImplicitSender with BeforeAndAfterAll {

  val tmpFolder = Files.createTempDirectory("hs")

  val fileDao = new FileDAO(tmpFolder.toString)
  val dsa = system.actorOf(BinaryFileManager.props(fileDao))

  val input = Array[Byte](0, 1, 2, 3, 4)
  val testInfo = fileDao.saveFile("hydra-spark", DateTime.now(), BinaryType.Data, input).get

  val jarInput = Array[Byte](0x50, 0x4b, 0x03, 0x04, 0x04)
  val jarFile = fileDao.saveFile("hydra-spark-jar", DateTime.now(), BinaryType.Jar, jarInput).get

  "A BinaryFileManager actor" must {

    "lists data files" in {
      dsa ! ListBinaries(None)
      expectMsg(Seq(testInfo, jarFile))
    }

    "lists data files by type" in {
      dsa ! ListBinaries(Some(BinaryType.Jar))
      expectMsg(Seq(jarFile))
    }

    "deletes a data file" in {
      val testInfo = fileDao.saveFile("hydra-spark-delete", DateTime.now(), BinaryType.Jar, input).get
      dsa ! DeleteBinary("hydra-spark-delete")
      expectMsg(Deleted)
    }

    "errors when deleting an unknown file" in {
      dsa ! DeleteBinary("hydra-spark-unknown")
      expectMsgPF() {
        case Error(e) if e.isInstanceOf[IllegalArgumentException] => true
      }
    }

    "errors if binary files is not in the right format" in {
      dsa ! StoreBinary("hydra-spark", BinaryType.Jar, testInfo.path)
      expectMsgPF() {
        case Error(e) => e.isInstanceOf[IllegalArgumentException] shouldBe true
      }
    }

    "errors if data files exists" in {
      dsa ! StoreBinary("hydra-spark-jar", BinaryType.Jar, jarFile.path)
      expectMsgPF() {
        case Error(e) => e.isInstanceOf[FileAlreadyExistsException] shouldBe true
      }
    }

    "stores data files" in {
      dsa ! StoreBinary("hydra-spark-new", BinaryType.Jar, jarFile.path)
      expectMsgPF() {
        case Stored(info) =>
          info.appName shouldBe "hydra-spark-new"
          info.path.indexOf("hydra-spark-new") should not be -1
      }
    }

  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

}
