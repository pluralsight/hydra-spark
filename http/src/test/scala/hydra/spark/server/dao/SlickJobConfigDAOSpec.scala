package hydra.spark.server.dao

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import hydra.spark.server.TestJars
import hydra.spark.server.io.SlickJobConfigRepository
import hydra.spark.server.model.JobStatus
import hydra.spark.server.sql.{FlywaySupport, H2Persistence}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

/**
  * Created by alexsilva on 6/2/17.
  */
class SlickJobConfigDAOSpec extends Matchers with FunSpecLike with H2Persistence with TestJars
  with ScalaFutures with BeforeAndAfterAll with DAOSpecBase {

  val config = ConfigFactory.load()

  var dao = new SlickJobConfigRepository

  val expectedConfig: Config = ConfigFactory.empty().withValue("marco", ConfigValueFactory.fromAnyRef("pollo"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    FlywaySupport.migrate(config.getConfig("h2-db"))
    whenReady(dao.saveJobConfig(jobId, expectedConfig)) { c =>
      c.jobConfig shouldBe expectedConfig
    }
  }

  describe("saveJobConfig() and getJobConfigs() tests") {

    it("should provide None on getJobConfig(jobId) where there is no config for a given jobId") {
      val config = dao.getJobConfig("44c32fe1-38a4-11e1-a06a-485d60c81a3e")
      whenReady(config) { c => c shouldBe None }
    }

    it("Save a new config, bring down DB, bring up DB, should get configs from DB") {
      val jobId2: String = genJobInfo(genJarInfo(false, false), false, false, JobStatus.Running, true).jobId
      val jobConfig2: Config = ConfigFactory.parseString("{merry=xmas}")
      val expectedConfig2 = ConfigFactory.empty().withValue("merry", ConfigValueFactory.fromAnyRef("xmas"))
      dao.saveJobConfig(jobId2, jobConfig2)
      dao = null
      dao = new SlickJobConfigRepository
      whenReady(dao.getJobConfig(jobId)) { cfg => cfg should equal(Some(expectedConfig)) }
      whenReady(dao.getJobConfig(jobId2)) { cfg => cfg should equal(Some(expectedConfig2)) }
    }
  }


}