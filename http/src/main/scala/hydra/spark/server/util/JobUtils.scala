package hydra.spark.server.util

import com.typesafe.config.Config
import configs.syntax._

/**
  * Created by alexsilva on 6/1/17.
  */
object JobUtils {

  def getMaxRunningJobs(config: Config): Int = {
    config.get[Int]("spark.server.max-jobs-per-context").valueOrElse(Runtime.getRuntime.availableProcessors)
  }
}
