package org.apache.spark.sql.jdbc

import hydra.avro.convert.IsoDate
import hydra.avro.util.SchemaWrapper
import hydra.spark.replication.kafka.KafkaStreamSource
import hydra.spark.replication.sinks.JdbcSink
import hydra.sql._
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import spray.json.{DefaultJsonProtocol, _}


class JdbcSinkProvider extends DataSourceRegister
  with StreamSinkProvider
  with DefaultJsonProtocol {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName, new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = IsoDate
  })

  override def shortName(): String = "hydra-jdbc-replicator"

  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    //create the initial tables from the topic subscription.
    //The other ones are created as needed by the JdbcRecordWriter.
    val pks = parameters("primaryKeys").parseJson.convertTo[Map[String, String]]

    val registry = new CachedSchemaRegistryClient(parameters("schema.registry.url"), 1000)

    parameters.get(KafkaStreamSource.InitialMatchingTopicsParam).map { topics =>
      topics.split(",").map { topic =>
        val topicPK = pks.find(_._1.matches(topic))
        createTopicTable(registry, parameters, topic, topicPK.map(_._2.split(",").toSeq)
          .getOrElse(Seq.empty))
      }
    }
    new JdbcSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  def createTopicTable(registry: SchemaRegistryClient,
                       parameters: Map[String, String], topic: String, pk: Seq[String]) = {
    val tblParams = parameters + (JDBCOptions.JDBC_TABLE_NAME -> topic)
    val options = new JDBCOptions(tblParams)
    val mode = hydra.avro.io.SaveMode.withName(parameters("saveMode"))

    val dialect = hydra.sql.JdbcDialects.get(options.url)
    val cp = new SparkConnectionProvider(options)
    val conn = cp.getConnection()
    try {
      val store = new JdbcCatalog(cp, UnderscoreSyntax, dialect)
      val meta = registry.getLatestSchemaMetadata(topic + "-value")
      val wrapper = SchemaWrapper.from(new Schema.Parser().parse(meta.getSchema()), pk)
      val isTruncate = options.isTruncate && isCascadingTruncateTable(options.url).contains(false)
      new TableCreator(cp, UnderscoreSyntax, dialect).createOrAlterTable(mode, wrapper, isTruncate)
    } finally {
      cp.close()
    }
  }

}