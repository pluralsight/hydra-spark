package hydra.spark.server.dao

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.ConfigFactory
import hydra.spark.server.TestJars
import hydra.spark.server.io.{BinaryInfo, SlickBinaryRepository}
import hydra.spark.server.job.BinaryType
import hydra.spark.server.sql.{FlywaySupport, H2Persistence}
import org.joda.time.DateTime
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by alexsilva on 6/2/17.
  */
class SlickBinaryDAOSpec extends Matchers with FunSpecLike with BeforeAndAfter with H2Persistence with TestJars
  with ScalaFutures with BeforeAndAfterAll with DAOSpecBase {

  val config = ConfigFactory.load()

  val dao = new SlickBinaryRepository("/tmp/hydra-spark-test/")
  val jarBytes: Array[Byte] = Files.toByteArray(testJar)
  var jarFile = new File(
    config.getString("hydra.spark.server.sql.rootdir"),
    jarInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".jar")

  val eggBytes: Array[Byte] = Files.toByteArray(emptyEgg)
  val eggInfo: BinaryInfo = BinaryInfo("myEggBinary", BinaryType.Egg, time)
  val eggFile = new File(config.getString("hydra.spark.server.sql.rootdir"),
    eggInfo.appName + "-" + jarInfo.uploadTime.toString("yyyyMMdd_hhmmss_SSS") + ".egg")

  override def beforeAll(): Unit = {
    super.beforeAll()
    FlywaySupport.migrate(config.getConfig("h2-db"))
  }

  before {
    jarFile.delete()
    eggFile.delete()
  }

  describe("save and get the jars") {

    it("should be able to save one jar and get it back") {
      val timeout = 10.seconds
      // check the pre-condition
      jarFile.exists() should equal(false)

      dao.saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes)

      val apps: Map[String, (BinaryType, DateTime)] = Await.result(dao.getApps, timeout)
        .filter(_._2._1 == BinaryType.Jar)

      jarFile.exists() should equal(true)
      apps.keySet should equal(Set(jarInfo.appName))
      apps(jarInfo.appName) should equal((BinaryType.Jar, jarInfo.uploadTime))
    }


    it("should retrieve the jar binary content for remote job manager") {
      // chack the pre-condition
      jarFile.exists() should equal(false)
      // retrieve the jar content
      val jarBinaryContent = dao.getBinaryContents(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime)
      whenReady(jarBinaryContent) { jar =>
        jarFile.exists() should equal(true)
        jar should equal(jarBytes)
      }
    }
  }

  describe("save and get Python eggs") {
    it("should be able to save one egg and get it back") {
      val timeout = 10.seconds
      // check the pre-condition
      eggFile.exists() should equal(false)

      // save
      dao.saveBinary(eggInfo.appName, BinaryType.Egg, eggInfo.uploadTime, eggBytes)

      // read it back
      val apps: Map[String, (BinaryType, DateTime)] =
        Await.result(dao.getApps, timeout).filter(_._2._1 == BinaryType.Egg)

      // test
      eggFile.exists() should equal(true)
      apps.keySet should equal(Set(eggInfo.appName))
      apps(eggInfo.appName) should equal((BinaryType.Egg, eggInfo.uploadTime))
    }
  }

  describe("delete binaries") {
    it("should be able to delete jar file") {
      val apps = dao.getApps
      whenReady(apps) { a =>
        a.keys should contain(jarInfo.appName)
      }

      whenReady(dao.deleteBinary(jarInfo.appName)) { _ =>
        whenReady(dao.getApps) { d =>
          d.keys should not contain (jarInfo.appName)
        }
      }
    }
  }
}
