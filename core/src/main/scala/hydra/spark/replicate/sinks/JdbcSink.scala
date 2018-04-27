package hydra.spark.replicate.sinks

import java.util.concurrent.Callable

import com.esotericsoftware.kryo.io.Input
import com.google.common.cache.CacheBuilder
import com.typesafe.config.ConfigFactory
import hydra.avro.io.{DeleteByKey, Upsert}
import hydra.avro.util.SchemaWrapper
import hydra.spark.replicate.SparkSingleton
import hydra.sql.{DriverManagerConnectionProvider, JdbcRecordWriter, JdbcWriterSettings}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.JavaConverters._

object JdbcSink {

  val SchemaRegistryUrlParam = "schema.registry.url"

  val schemaCache = CacheBuilder.newBuilder.maximumSize(1000).build[String, Schema]()

  val writerCache = CacheBuilder.newBuilder.maximumSize(1000).build[String, JdbcRecordWriter]()

  def getInitialSchema(topic: String, registry: SchemaRegistryClient): Schema = {
    schemaCache.get(topic, new Callable[Schema] {
      override def call(): Schema =
        new Schema.Parser()
          .parse(registry.getLatestSchemaMetadata(topic + "-value").getSchema)
    })
  }

  def getWriter(topic: String, initialSchema: Schema, pk: String, params: Map[String, String]) = {
    writerCache.get(topic, new Callable[JdbcRecordWriter] {
      override def call(): JdbcRecordWriter = {
        val wrapper = Option(if (pk.isEmpty) null else pk)
          .map(p => SchemaWrapper.from(initialSchema, p.split(",")))
          .getOrElse(SchemaWrapper.from(initialSchema))
        val config = ConfigFactory.parseMap(params.asJava)
        val provider = DriverManagerConnectionProvider(config)
        new JdbcRecordWriter(JdbcWriterSettings(config), provider, wrapper)
      }
    })
  }

  def flushWriters(): Unit =
    writerCache.asMap().asScala.foreach(_._2.flush())
}

class JdbcSink(sqlContext: SQLContext,
               parameters: Map[String, String],
               partitionColumns: Seq[String],
               outputMode: OutputMode) extends Sink with Logging {
  val options = new JDBCOptions(parameters)
  val sinkLog = new JDBCSinkLog(parameters, sqlContext.sparkSession)
  // If user specifies a batchIdCol in the parameters, then it means that the user wants exactly
  // once semantics. This column will store the batch Id for the row when an uncommitted batch
  // is replayed, JDBC SInk will delete the rows that were added to the previous play of the
  // batch
  val batchIdCol = parameters.get("batchIdCol")
  val conf = sqlContext.sparkContext.getConf

  require(parameters.isDefinedAt(JdbcSink.SchemaRegistryUrlParam),
    s"Option '${JdbcSink.SchemaRegistryUrlParam}' is required.")

  def addBatch(batchId: Long, df: DataFrame): Unit = {

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      if (sinkLog.isBatchCommitted(batchId, conn)) {
        logInfo(s"Skipping already committed batch $batchId")
      } else {
        sinkLog.startBatch(batchId, conn)
        saveRows(df, options, batchId)
      }
      sinkLog.commitBatch(batchId, conn)

    } finally {
      conn.close()
    }
  }

  def saveRows(df: DataFrame,

               options: JDBCOptions,
               batchId: Long): Unit = {
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }

    val lconf = conf
    val registryUrl = parameters("schema.registry.url")
    val localParams = parameters

    val registry = SparkSingleton {
      new CachedSchemaRegistryClient(registryUrl, 1000)
    }

    if (batchIdCol.isEmpty) {
      repartitionedDF.queryExecution.toRdd.foreachPartition(it => {
        val kryo = new KryoSerializer(lconf).newKryo()

        it.foreach { row =>
          val pk = row.getString(4)
          val topic = row.getString(0)
          val input = new Input(row.getBinary(3))
          val payload = kryo.readClassAndObject(input).asInstanceOf[GenericRecord]
          val initialSchema = JdbcSink.getInitialSchema(topic, registry.get)
          val writer = JdbcSink.getWriter(topic, initialSchema, pk, localParams)
          val operation = Option(payload).map(p => Upsert(p)) orElse {
            Option(if (pk.isEmpty) null else pk).map(pk => DeleteByKey(Map(pk -> payload.get(pk))))
          }
          operation.foreach(op => writer.batch(op))
          println(payload.toString)

        }

        JdbcSink.flushWriters()
      })
    }
  }

  def saveMode(outputMode: OutputMode): SaveMode = {
    if (outputMode == OutputMode.Append()) {
      SaveMode.Append
    } else if (outputMode == OutputMode.Complete()) {
      SaveMode.Overwrite
    } else {
      throw new IllegalArgumentException(s"Output mode $outputMode is not supported by JdbcSink")
    }
  }

}

