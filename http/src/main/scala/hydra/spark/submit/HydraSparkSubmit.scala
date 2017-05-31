package hydra.spark.submit

import java.io.File

import hydra.spark.api.DispatchDetails
import hydra.spark.configs.ConfigSupport
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.util.Try

/**
  * Created by alexsilva on 5/1/17.
  */
case class HydraSparkSubmit[T](dispatch: DispatchDetails[T]) {

  import HydraSparkSubmit._

  val logFile = new File(baseLogDir, s"${dispatch.name}-${System.currentTimeMillis()}.log")

  lazy val launcher: Try[SparkLauncher] = {
    Try(Seq(sparkHome, hydraSparkJar).map(_.get)).map { s =>
      val info = SparkSubmitInfo(s(0), s(1), hadoopConfDir, yarnConfDir)
      HydraSparkLauncher.createLauncher(info, dispatch)
    }
  }

  def submit(listeners: SparkAppHandle.Listener*): Try[SparkAppHandle] = {
    launcher.map { l =>
      l.redirectOutput(logFile)
      l.startApplication(listeners: _*)
    }
  }
}

object HydraSparkSubmit extends ConfigSupport {

  import configs.syntax._

  val baseLogDir = config.get[String]("hydra.spark.log.dir").valueOrElse("/var/log/hydra-spark")

  val sparkHome = Try(config.get[String]("spark.home")
    .valueOrThrow(e => HydraSparkSubmitException("Unable to find spark home config.", e.configException)))

  val hydraSparkJar = Try(config.get[String]("hydra.spark.assembly")
    .valueOrThrow(e => HydraSparkSubmitException("Unable to find hydra-spark jar config.", e.configException)))

  val hadoopConfDir = config.get[String]("spark.HADOOP_CONF_DIR").toOption

  val yarnConfDir = config.get[String]("spark.YARN_CONF_DIR").toOption

}

case class HydraSparkSubmitException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
