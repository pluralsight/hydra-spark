package hydra.spark.server.model

import com.typesafe.config.Config

case class JobConfig(configId:Option[Int], jobId: String, jobConfig: Config)