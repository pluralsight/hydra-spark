package hydra.spark.replication.kafka

import configs.syntax._
import hydra.avro.io.SaveMode
import hydra.spark.api._
import hydra.spark.configs.ConfigSupport
import org.apache.avro.generic.GenericRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import spray.json.DefaultJsonProtocol

import scala.util.Try

class KafkaReplicationJob(details: ReplicationDetails) extends HydraSparkJob with ConfigSupport
  with Logging with DefaultJsonProtocol {

  import spray.json._

  override val id: String = details.name

  override val author: String = "TBD"

  private val schemaRegistryUrl = config.get[String]("hydra.schema.registry.url")
    .valueOrThrow(e => InvalidDslException("A schema registry url is required."))

  private val connectionUrl = details.connectionInfo.get("url")
    .getOrElse(throw new InvalidDslException("A connection url is required."))

  lazy val source = new KafkaStreamSource(details.topics,
    config.getString("kafka.bootstrap.servers"),
    schemaRegistryUrl,
    details.primaryKeys,
    details.startingOffsets)

  private lazy val checkpointDir = s"${
    config.get[String]("spark.checkpoint.dir")
      .valueOrElse("/tmp")
  }/$id"


  lazy val spark = SparkSession
    .builder
    .master("local[4]")
    .config("spark.kryoserializer.buffer.max", "128m")
    .config("spark.kryo.classesToRegister", classOf[GenericRecord].getName)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "true")
    .appName(id)
    .getOrCreate()

  private val matchingTopics = source.initialMatchingTopics

  log.debug(s"Initial matching topics: ${matchingTopics.mkString(",")}")

  private lazy val stream =
    source.stream(spark)
      .writeStream
      .format("hydra-jdbc-replicator")
      .option("connection.url", connectionUrl)
      .option("driver", details.connectionInfo.getOrElse("driver", "org.postgresql.Driver"))
      .option("url", connectionUrl) //for spark
      .option("connection.user", details.connectionInfo.get("user").getOrElse(""))
      .option("connection.password", details.connectionInfo.get("password").getOrElse(""))
      .option("checkpointLocation", checkpointDir)
      .option("saveMode", details.saveMode)
      .option(KafkaStreamSource.InitialMatchingTopicsParam, matchingTopics.mkString(","))
      .option(KafkaStreamSource.PrimaryKeysParam, details.primaryKeys.toJson.compactPrint)
      .option("schema.registry.url", schemaRegistryUrl)
      .trigger(Trigger.ProcessingTime("50 seconds"))


  private var streamingQuery: StreamingQuery = _

  @volatile private var started = false

  override def run(): Unit = synchronized {
    if (started) throw new QueryExecutionException(s"Job $id is already running.")
    streamingQuery = stream.start()
    started = true
  }

  override def awaitTermination(): Unit = streamingQuery.awaitTermination()

  override def stop(): Unit = streamingQuery.stop()

  override def validate: ValidationResult = {
    tryValidate(() => SaveMode.withName(details.saveMode.capitalize),
      s"${details.saveMode} is not a valid save mode. Valid options are: ${SaveMode.values.mkString(",")}")
      .flatMap { _ =>
        tryValidate(() => details.connectionInfo("url"), "A connection url is required.")
      }.get
  }


  private def tryValidate(op: () => Any, err: String) = {
    Try(op.apply).map(_ => Valid).recover {
      case _: Throwable => Invalid(ValidationError("kafka", err))
    }
  }
}

