package hydra.spark.replicate.kafka

import hydra.spark.api.{HydraSparkJob, ReplicationDetails, Valid, ValidationResult}
import hydra.spark.configs.ConfigSupport
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.streaming.StreamingQuery
import configs.syntax._

class KafkaReplicationJob(details: ReplicationDetails) extends HydraSparkJob with ConfigSupport {

  override val id: String = details.name

  override val author: String = "TBD"

  lazy val source = new KafkaStreamSource(details.topics,
    config.getString("schema.registry.url"),
    config.getString("kafka.bootstrap.servers"),
    details.primaryKeys,
    details.startingOffsets)

  private lazy val checkpointDir = s"${config.get[String]("spark.checkpoint.dir").valueOrElse("/tmp")}/$id"

  lazy val spark = SparkSession
    .builder
    .master("local[4]")
    .config("spark.kryoserializer.buffer.max", "128m")
    .config("spark.kryo.classesToRegister", classOf[GenericRecord].getName)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "true")
    .appName(id)
    .getOrCreate()

  private lazy val stream =
    source.stream(spark)
      .writeStream
      .format("hydra-jdbc-replicator")
      .option("connection.url", details.connectionInfo.get[String]("url").valueOrElse(""))
      .option("checkpointLocation", checkpointDir)
      .option("schema.registry.url", config.getString("schema.registry.url"))

  private var streamingQuery: StreamingQuery = _

  @volatile private var started = false

  override def run(): Unit = synchronized {
    if (started) throw new QueryExecutionException(s"Job $id is already running.")
    streamingQuery = stream.start()
    started = true
  }

  override def awaitTermination(): Unit = streamingQuery.awaitTermination()

  override def stop(): Unit = streamingQuery.stop()

  override def validate: ValidationResult = Valid //validation should happen on parsing
}
