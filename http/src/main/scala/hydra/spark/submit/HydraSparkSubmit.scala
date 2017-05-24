package hydra.spark.submit

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import hydra.spark.api.DispatchDetails
import hydra.spark.submit.HydraSparkSubmit.baseLogDir
import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.dsl

/**
  * Created by alexsilva on 5/1/17.
  */
case class HydraSparkSubmit[T](dispatch: DispatchDetails[T]) {

  import HydraSparkSubmit._

  val logFile = new File(baseLogDir, s"${dispatch.name}-${System.currentTimeMillis()}.log")


  val dslWithDefaults = catalyst.dsl.withFallback(defaults)

  val sparkConf = rootConfig.getConfig("spark").atKey("spark")

  lazy val launcher = {
    val info = SparkSubmitInfo(sparkHome, hydraSparkJar, hadoopConfDir, yarnConfDir)
    HydraSparkLauncher.createLauncher(sparkConf, info, catalyst.dsl.root().render(ConfigRenderOptions.concise()))
  }

  def submit(listeners: SparkAppHandle.Listener*) = {
    launcher.redirectOutput(logFile)
    launcher.startApplication(listeners: _*)
  }
}

object HydraSparkSubmit {

  import configs.syntax._

  val baseLogDir = applicationConfig.get[String]("transport.spark.log.dir").valueOrElse("/var/log/hydra/transports")


  val defaults = rootConfig.resolve().get[Config]("hydra.transport.defaults")
    .valueOrElse(ConfigFactory.empty).atKey("hydra.transport.defaults")

  val sparkHome = rootConfig.get[String]("spark.home")
    .valueOrThrow(e => new JobExecutionException("Unable to find spark home config.", e.configException))

  val hydraSparkJar = applicationConfig.get[String]("transport.spark-dsl.jar")
    .valueOrThrow(e => new JobExecutionException("Unable to find hydra-spark jar config.", e.configException))

  val hadoopConfDir = rootConfig.get[String]("spark.HADOOP_CONF_DIR").toOption

  val yarnConfDir = rootConfig.get[String]("spark.YARN_CONF_DIR").toOption

}

