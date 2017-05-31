package hydra.spark.configs

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by alexsilva on 5/25/17.
  */
trait ConfigSupport {
  val config = ConfigFactory.load().withFallback(ConfigFactory.parseFile(new File("/etc/hydra/hydra-spark.conf")))
}
