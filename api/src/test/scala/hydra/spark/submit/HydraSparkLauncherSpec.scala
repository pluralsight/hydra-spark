package hydra.spark.submit

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 1/18/17.
  */
class HydraSparkLauncherSpec extends Matchers with FunSpecLike {
  describe("When creating a launcher") {

    val sparkInfo = SparkSubmitInfo("/spark/home", "hydra-spark.jar", Some("/hadoop"), Some("/yarn"))


    val dsl =
      """
        |    {
        |    	"dispatch": {
        |    		"version": 1,
        |      "env":{
        |         "HADOOP_USER_NAME":"hydra"
        |       },
        |    		"spark.master": "local[1]",
        |    		"name": "test-dispatch",
        |    		"source": {
        |    			"hydra.spark.testutils.ListSource": {
        |    				"messages": ["1", "2", "3"]
        |    			}
        |    		},
        |    		"operations": {
        |    			"print-rows": {}
        |    		}
        |    	}
        |    }
      """.stripMargin

    it("builds the right environment variables") {
      val dslC = ConfigFactory.parseString(dsl).getConfig("dispatch")

      val env = HydraSparkLauncher.buildEnv(dslC, sparkInfo)

      env shouldBe Map("HADOOP_CONF_DIR" -> "/hadoop", "YARN_CONF_DIR" -> "/yarn", "HADOOP_USER_NAME" -> "hydra")
    }

    it("submits") {
      val launcher = HydraSparkLauncher.createLauncher(
        HydraSparkLauncher.defaultSparkCfg,
        sparkInfo,
        dsl
      )

      import org.scalatest.PrivateMethodTester._
      val createBuilder = PrivateMethod[ProcessBuilder]('createBuilder)
      val builder = launcher invokePrivate createBuilder()
      val cmds = builder.command().asScala
      cmds should contain("test-dispatch")
      cmds should contain("spark.master=local[1]")
    }
  }
}
