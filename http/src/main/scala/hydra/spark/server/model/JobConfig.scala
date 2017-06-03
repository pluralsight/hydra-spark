package hydra.spark.server.model

import com.typesafe.config.Config

case class JobConfig(jobId: String, jobConfig: Config)