package hydra.spark.testutils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}
import java.util

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import kafka.api.OffsetRequest
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.scalatest.Suite

import scala.io.Source

/**
  * Created by alexsilva on 4/5/17.
  */
trait KafkaTestSupport extends EmbeddedKafka {

  this: Suite =>

  val bootstrapServers = "localhost:6001"

  val zkConnect = "localhost:6000"

  val avroProps = Map("format" -> "avro", "start" -> OffsetRequest.EarliestTime)

  val jsonProps = Map("format" -> "string", "start" -> OffsetRequest.EarliestTime)

  val schemaRegistry = new RestApp(8081, zkConnect,
    SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC, AvroCompatibilityLevel.BACKWARD.name, true)

  val schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)

  val schema = new Schema.Parser parse Source.fromInputStream(getClass.getResourceAsStream("/schema.avsc")).mkString

  def startKafka() = {
    EmbeddedKafka.start()
    schemaRegistry.start()
    schemaRegistryClient.register("test-schema-value", schema)
    publishAvro("testAvro")
    publishJson("testJson")
  }

  def stopKafka() = {
    EmbeddedKafka.stop()
    schemaRegistry.stop()
  }

  def publishJson(topic: String, n: Int = 10): Seq[String] = {
    val msgs: Seq[String] = for (i <- 0 to n) yield {
      s"""{"messageId": $i,"messageValue":"hello"}"""
    }

    msgs.foreach(EmbeddedKafka.publishStringMessageToKafka(topic, _))

    msgs
  }

  def publishJsonWithKeys(topic: String, n: Int = 10): Seq[String] = {
    val msgs: Seq[String] = for (i <- 0 to n) yield {
      s"""{"messageId": $i,"messageValue":"hello"}"""
    }
    msgs.zipWithIndex.foreach { x =>
      EmbeddedKafka.publishToKafka(topic, x._2.toString, x._1)(implicitly[EmbeddedKafkaConfig], new StringSerializer, new StringSerializer)
    }
    msgs
  }

  def publishAvro(topic: String, n: Int = 10): Seq[GenericRecord] = {
    val cfg = implicitly[EmbeddedKafkaConfig]
    val reader = new GenericDatumReader[Object](schema)
    val msgs: Seq[GenericRecord] = for (i <- 0 to n) yield {
      val json = s"""{"messageId": $i,"messageValue":"hello"}"""
      val in = new ByteArrayInputStream(json.getBytes)
      val din = new DataInputStream(in)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      reader.read(null, decoder).asInstanceOf[GenericRecord]
    }

    msgs.foreach(EmbeddedKafka.publishToKafka(topic, _)(cfg, new TestKafkaAvroSerializer))

    msgs
  }

  def publishAvroWithKeys(topic: String, n: Int = 10): Seq[GenericRecord] = {
    val reader = new GenericDatumReader[Object](schema)
    val msgs: Seq[GenericRecord] = for (i <- 0 to n) yield {
      val json = s"""{"messageId": $i,"messageValue":"hello"}"""
      val in = new ByteArrayInputStream(json.getBytes)
      val din = new DataInputStream(in)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      reader.read(null, decoder).asInstanceOf[GenericRecord]
    }

    msgs.zipWithIndex.foreach { x =>
      EmbeddedKafka.publishToKafka(topic, x._2.toString, x._1)(implicitly[EmbeddedKafkaConfig], new StringSerializer,
        new TestKafkaAvroSerializer)
    }
    msgs
  }
}


class TestKafkaAvroSerializer[T <: GenericRecord]()
  extends Serializer[T] {

  private val encoder = new TestKafkaAvroEncoder[T]()

  override def serialize(topic: String, data: T): Array[Byte] =
    encoder.toBytes(data)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}

class TestKafkaAvroEncoder[T <: GenericRecord](props: VerifiableProperties = null)
  extends Encoder[T] {
  private val NoEncoderReuse = null.asInstanceOf[BinaryEncoder]

  override def toBytes(nullableData: T): Array[Byte] = {
    Option(nullableData).fold[Array[Byte]](null) { data =>
      val writer: DatumWriter[T] = new SpecificDatumWriter[T](data.getSchema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, NoEncoderReuse)

      writer.write(data, encoder)
      encoder.flush()
      out.close()

      out.toByteArray
    }
  }
}