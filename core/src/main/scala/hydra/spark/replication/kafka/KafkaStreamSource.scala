package hydra.spark.replication.kafka

import java.io.ByteArrayOutputStream
import java.util.Properties

import hydra.common.util.TryWith
import hydra.spark.internal.Logging
import hydra.spark.replication.SparkSingleton
import hydra.spark.replication.kafka.KafkaStreamSource.KafkaRecord
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.collection.JavaConverters._

/**
  * @param topics
  * @param bootstrapServers
  * @param schemaRegistryUrl
  * @param primaryKeys
  * @param startingOffsets Starting can be set as follows:
  *                        earliest or json string {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
  */
class KafkaStreamSource(topics: Either[List[String], String],
                        bootstrapServers: String,
                        schemaRegistryUrl: String,
                        primaryKeys: Map[String, String],
                        startingOffsets: String = "earliest") extends Logging {

  lazy val initialMatchingTopics = topics match {
    case Right(pattern) =>
      val consumerProps = new Properties()
      consumerProps.put("bootstrap.servers", bootstrapServers)
      consumerProps.put("value.deserializer", classOf[StringDeserializer].getName)
      consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
      TryWith(new KafkaConsumer[Any, Any](consumerProps)) { kc =>
        kc.listTopics().asScala.filter(_._1.matches(pattern)).keys.toSeq
      }.recover { case e: Throwable => log.error("KafkaSource", e); Seq.empty }.get
    case Left(topics) => topics
  }

  def stream(spark: SparkSession): Dataset[KafkaRecord] = {
    import spark.implicits._
    val pks = primaryKeys

    val subscription = topics match {
      case Right(pattern) => "subscribePattern" -> pattern
      case Left(topics) => "subscribe" -> topics.mkString
    }

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscription._1, subscription._2)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", 5)
      .load()


    val kryo = new KryoSerializer(spark.sparkContext.getConf)
    val url = schemaRegistryUrl
    val avroDeserializer = SparkSingleton {
      new KafkaAvroDeserializer(new CachedSchemaRegistryClient(url, 1000))
    }
    stream.select("topic", "offset", "key", "value")
      .as[(String, Long, String, Array[Byte])]
      .map { r =>
        val genericRecord = avroDeserializer.get.deserialize(r._1, r._4).asInstanceOf[GenericRecord]
        val bao = new ByteArrayOutputStream()
        val output = kryo.newKryoOutput()
        val serializer = kryo.newKryo()
        output.setOutputStream(bao)
        serializer.writeClassAndObject(output, genericRecord)
        output.flush()
        val buffer = bao.toByteArray
        output.close()
        (r._1, r._2, r._3, buffer, pks.get(r._1).getOrElse(""))
      }(KafkaStreamSource.kafkaRecordEncoder)
  }
}

object KafkaStreamSource {
  implicit def kafkaRecordEncoder[A1, A2, A3, A4, A5](
                                                       implicit e1: Encoder[A1],
                                                       e2: Encoder[A2],
                                                       e3: Encoder[A3],
                                                       e4: Encoder[A4],
                                                       e5: Encoder[A5]
                                                     ): Encoder[(A1, A2, A3, A4, A5)] = Encoders.tuple[A1, A2, A3, A4, A5](e1, e2, e3, e4, e5)

  /**
    * topic,offset,payload,pk
    */
  type KafkaRecord = (String, Long, String, Array[Byte], String)

  val InitialMatchingTopicsParam: String = "initialMatchingTopics"
  val PrimaryKeysParam: String = "primaryKeys"
}
