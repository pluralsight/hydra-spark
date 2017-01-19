package hydra.spark.submit

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.configs._
import hydra.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 1/18/17.
  */
object HydraSparkLauncher extends Logging {


  def buildEnv(dispatchConfig: Config, sparkInfo: SparkSubmitInfo): Map[String, String] = {
    val env: Map[String, String] = Map(
      "HADOOP_CONF_DIR" -> sparkInfo.hadoopConfDir,
      "YARN_CONF_DIR" -> sparkInfo.yarnConfDir
    ).collect {
      case (key, Some(value)) => key -> value
    }

    env ++ dispatchConfig.flattenAtKey("env").map { case (k, v) => k.substring(4) -> v }

  }

  def createLauncher(baseSparkConfig: Config, sparkInfo: SparkSubmitInfo, dsl: String): SparkLauncher = {

    val dslC = ConfigFactory.parseString(dsl).getConfig("dispatch")

    val sparkConf = ConfigFactory.parseMap(dslC.flattenAtKey("spark").asJava)
      .withFallback(baseSparkConfig)
      .withFallback(defaultSparkCfg)

    val launcher = new SparkLauncher(buildEnv(dslC, sparkInfo).asJava)
      .setSparkHome(sparkInfo.sparkHome)
      .setAppResource(sparkInfo.hydraSparkJar)
      .setAppName(dslC.get[String]("name").getOrElse(UUID.randomUUID().toString))
      .setMainClass("hydra.spark.submit.DslRunner")
      .addAppArgs(dsl)
      .setMaster(sparkConf.getString("spark.master"))
      .setVerbose(true)

    sparkConf.entrySet().asScala.foreach(entry => launcher.setConf(entry.getKey, entry.getValue.unwrapped().toString))

    launcher
  }


  val defaultSparkCfg = ConfigFactory.parseMap(
    Map(
      "spark.ui.enabled" -> "false",
      "spark.yarn.queue" -> "hydra.dispatch",
      "spark.yarn.am.memory" -> "1g",
      "spark.driver.memory" -> "1g",
      "spark.akka.frameSize" -> "200",
      "spark.executor.memory" -> "8g",
      "spark.executor.instances" -> "8",
      "spark.executor.cores" -> "12",
      "spark.default.parallelism" -> "5000"
    ).asJava
  )

}


case class SparkSubmitInfo(sparkHome: String, hydraSparkJar: String, hadoopConfDir: Option[String],
                           yarnConfDir: Option[String])
