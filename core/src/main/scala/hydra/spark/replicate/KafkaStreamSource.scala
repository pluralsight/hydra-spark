package hydra.spark.replicate

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{Dataset, SparkSession}

class KafkaStreamSource(topics: Either[List[String], String], bootstrapServers: String,
                        primaryKeys: Map[String, String]) {


  def stream(spark: SparkSession): Dataset[KafkaRecord[String]] = {
    import spark.implicits._
    val pks = primaryKeys
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[KafkaRecord[String]]

    val subscription = topics match {
      case Right(pattern) => "subscribePattern" -> pattern
      case Left(topics) => "subscribe" -> topics.mkString
    }

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscription._1, subscription._2)
      .option("startingOffsets", getStartOffset())
      .option("maxOffsetsPerTrigger", 100000L)
      .load()

    val avroDeserializer = SparkSingleton {
      new KafkaAvroDeserializer(new CachedSchemaRegistryClient("http://dvs-schema-registry-stage.vnerd.com:8081", 1000))
    }

    stream.select("topic", "offset", "key", "value")
      .as[(String, Long, String, Array[Byte])]
      .map { r =>
        KafkaRecord(r._1, r._2, r._3,
          avroDeserializer.get.deserialize(r._1, r._4).asInstanceOf[GenericRecord], pks.get(r._1))
      }(encoder)
  }

  // Starting and Ending Offsets can be set as follows:
  //   earliest, latest, or json string {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
  def getStartOffset() = "earliest"

  def getEndOffset() = "latest"
}

case class KafkaRecord[K](topic: String, offset: Long, key: K, payload: GenericRecord,
                          primaryKeyColumn: Option[String])
