package hydra.spark.sources

import com.typesafe.config.ConfigFactory
import hydra.spark.replication.kafka.KafkaStreamSource
import org.apache.avro.generic.GenericRecord
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
    .config("spark.kryoserializer.buffer.max", "128m")
    .config("spark.kryo.classesToRegister", classOf[GenericRecord].getName)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "true")
    .appName("KafkaRouterStructuredStream")
    .getOrCreate()


  val stream = new KafkaStreamSource(Right("exp.identity.UserSignedIn"),
    config.getString("kafka.bootstrap.servers"), config.getString("schema.registry.url"),
    Map.empty).stream(spark)

  stream.writeStream
    .format("hydra-jdbc-replicator")
    .option("connection.url", "jdbc:postgresql://datahub-relational.cozqodssr8yj.us-west-2.rds.amazonaws.com/prod?user=dh_admin&password=XRFVQXQ34HX36186")
    .option("url", "jdbc:postgresql://datahub-relational.cozqodssr8yj.us-west-2.rds.amazonaws.com/prod?user=dh_admin&password=XRFVQXQ34HX36186")
    .option("dbtable", "test")
    .option("checkpointLocation", "/tmp")
    .option("schema.registry.url", "http://dvs-schema-registry-stage.vnerd.com:8081/")
    .start().awaitTermination()


}

