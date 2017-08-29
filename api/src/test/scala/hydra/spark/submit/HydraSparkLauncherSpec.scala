package hydra.spark.submit

import com.typesafe.config.ConfigFactory
import hydra.spark.api.TransformationDetails
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
      """.stripMargin

    val dslC = ConfigFactory.parseString(dsl)

    it("builds the right environment variables") {

      val d = TransformationDetails(EmptySource("test"), Seq(NoOpOperation), false, dslC)

      val env = HydraSparkLauncher.env(d, sparkInfo)

      env shouldBe Map("HADOOP_CONF_DIR" -> "/hadoop", "YARN_CONF_DIR" -> "/yarn", "HADOOP_USER_NAME" -> "hydra")
    }

    it("submits") {
      val d = TransformationDetails(EmptySource("test"), Seq(NoOpOperation), false, dslC)
      val launcher = HydraSparkLauncher.createLauncher(sparkInfo, d)

      import org.scalatest.PrivateMethodTester._
      val createBuilder = PrivateMethod[ProcessBuilder]('createBuilder)
      val builder = launcher invokePrivate createBuilder()
      val cmds = builder.command().asScala
      cmds should contain("spark.master=local[1]")
    }
  }
}
