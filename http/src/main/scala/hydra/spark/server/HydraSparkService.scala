package hydra.spark.server

import com.github.vonnagy.service.container.ContainerBuilder
import com.github.vonnagy.service.container.service.ContainerService
import com.typesafe.config.ConfigFactory
import hydra.spark.server.endpoints.DslEndpoint

/**
  * Created by alexsilva on 4/28/17.
  */
object HydraSparkService extends App {

  val config = ConfigFactory.load()

  val endpoints = Seq(classOf[DslEndpoint])

  def buildContainer(): ContainerService = {
    val builder = ContainerBuilder()
      .withConfig(config)
      .withRoutes(endpoints: _*)
      .withName("hydra-spark")

    builder.build
  }

  buildContainer().start()

}
