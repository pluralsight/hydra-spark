package hydra.spark.api

import com.typesafe.config.Config

/**
  * Created by alexsilva on 6/1/17.
  */
trait JobEnvironment {
  def jobId: String

  def contextConfig: Config
}
