package hydra.spark.sources

import com.typesafe.config.ConfigFactory
import hydra.spark.replicate.{JdbcUpsertWriter, KafkaStreamSource}
import org.apache.spark.sql.SparkSession

object Tester extends App {

  val config = ConfigFactory.parseString(
    """
      |schema.registry.url = "http://dvs-schema-registry-stage.vnerd.com:8081/"
      |kafka.bootstrap.servers="10.107.222.229:9092,10.107.219.251:9092"
      |connection.url = "jdbc:postgresql://datahub-relational.cozqodssr8yj.us-west-2.rds.amazonaws.com/prod?user=dh_admin&password=XRFVQXQ34HX36186"
    """.stripMargin)

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("KafkaRouterStructuredStream")
    .getOrCreate()



  val stream = new KafkaStreamSource(Right("exp.identity.UserSignedIn"),
    config.getString("kafka.bootstrap.servers"),
    Map("exp.identity.UserSignedIn" -> "handle")).stream(spark)

  stream.writeStream
    .queryName("test")
    .foreach(new JdbcUpsertWriter(config))
    .start().awaitTermination()


}

