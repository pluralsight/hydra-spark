package hydra.spark.server.actors

import java.nio.file.{FileAlreadyExistsException, Files}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import DataFileManager._
import hydra.spark.server.io.FileDAO
import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by alexsilva on 5/31/17.
  */
class DataFileManagerSpec extends TestKit(ActorSystem("hydra-spark")) with WordSpecLike with Matchers
  with ImplicitSender with BeforeAndAfterAll {

  val tmpFolder = Files.createTempDirectory("hs")

  val fileDao = new FileDAO(tmpFolder.toString)
  val dsa = system.actorOf(DataFileManager.props(fileDao))

  val input = Array[Byte](0, 1, 2, 3, 4)
  val testInfo = fileDao.saveFile("hydra-spark", DateTime.now(), BinaryType.Data, input).get

  "A DataFileStorage actor" must {

    "lists data files" in {
      dsa ! ListDataFiles
      expectMsg(Seq(testInfo))
    }

    "deletes a data file" in {
      val testInfo = fileDao.saveFile("hydra-spark-delete", DateTime.now(), BinaryType.Data, input).get
      dsa ! DeleteDataFile("hydra-spark-delete")
      expectMsg(Deleted)
    }

    "errors when deleting an unknown file" in {
      dsa ! DeleteDataFile("hydra-spark-unknown")
      expectMsgPF() {
        case Error(e) if e.isInstanceOf[IllegalArgumentException] => true
      }
    }

    "errors if data files exists" in {
      dsa ! StoreDataFile("hydra-spark", testInfo.path)
      expectMsgPF() {
        case Error(e) => e.isInstanceOf[FileAlreadyExistsException] shouldBe true
      }
    }

    "stores data files" in {
      dsa ! StoreDataFile("hydra-spark-new", testInfo.path)
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
