package hydra.spark.configs

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import configs.syntax._
import org.apache.spark.SparkConf

/**
  * Created by alexsilva on 5/25/17.
  */
trait ConfigSupport {
  val config = ConfigFactory.load().withFallback(ConfigFactory.parseFile(new File("/etc/hydra/hydra-spark.conf")))

  val sparkDefaults = config.get[Config]("spark").valueOrElse(ConfigFactory.empty).atKey("spark")

  def sparkConf(dsl: Config, name: String) = {
    import hydra.spark.configs._
    val sparkDslConf = dsl.flattenAtKey("spark")
    val sparkConf = sparkDslConf ++ sparkDefaults.flattenAtKey("spark")
    val jars = dsl.get[List[String]]("spark.jars").valueOrElse(List.empty)
    val appName = sparkDslConf.get("spark.app.name").getOrElse(name)

    new SparkConf().setAll(sparkConf)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.avro.generic.GenericData.Record]))
      .setAppName(appName).setJars(jars)
  }
}
