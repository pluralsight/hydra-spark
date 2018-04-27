package hydra.spark.replicate

import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.Config
import hydra.avro.io.{DeleteByKey, Operation, SaveMode, Upsert}
import hydra.avro.util.SchemaWrapper
import hydra.spark.replicate.KafkaStreamSource.KafkaRecord
import hydra.sql.{DriverManagerConnectionProvider, JdbcRecordWriter, JdbcWriterSettings}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ForeachWriter

import scala.collection.JavaConverters._

class JdbcUpsertWriter(config: Config) extends ForeachWriter[KafkaRecord] with Logging {

  var writers: Cache[String, JdbcRecordWriter] = _

  lazy val registryClient = SparkSingleton {
    new CachedSchemaRegistryClient(config.getString("schema.registry.url"), 1000)
  }


  override def open(partitionId: Long, version: Long) = {
    writers = CacheBuilder.newBuilder.maximumSize(1000).build[String, JdbcRecordWriter]()
    true
  }


  override def process(record: KafkaRecord) = {

    val payload = record._4
    val primaryKey = record._5
    val topic = record._1

    val op: Option[Operation] = Option(payload).map(p => Upsert(p)) orElse {
      primaryKey.map(pk => DeleteByKey(Map(pk -> payload.get(pk))))
    }

    op.foreach(o => writers.get(topic, createWriter(topic,
      primaryKey)).batch(o))
  }

  override def close(errorOrNull: Throwable) = {
    println("CLOSING")
    writers.asMap().asScala.foreach(_._2.flush())
  }


  private def createWriter(topic: String, pk: Option[String]) = new Callable[JdbcRecordWriter] {
    override def call(): JdbcRecordWriter = {
      //we get the latest schema
      val schema = new Schema.Parser()
        .parse(registryClient.get.getLatestSchemaMetadata(topic + "-value").getSchema)

      val wrapper = pk.map(p => SchemaWrapper.from(schema, p.split(",")))
        .getOrElse(SchemaWrapper.from(schema))

      log.debug(s"Creating new JdbcRecordWriter for topic ${topic}")
      new JdbcRecordWriter(
        JdbcWriterSettings(config),
        DriverManagerConnectionProvider(config),
        wrapper,
        SaveMode.Append)

    }
  }
}