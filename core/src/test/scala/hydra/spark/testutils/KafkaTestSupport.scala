package hydra.spark.testutils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}
import java.util

import kafka.api.OffsetRequest
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.Serializer
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by alexsilva on 4/5/17.
  */
trait KafkaTestSupport extends EmbeddedKafka with BeforeAndAfterAll {

  this: Suite =>

  val avroTopic = Map("format" -> "json", "start" -> OffsetRequest.EarliestTime)
  val jsonTopic = Map("format" -> "string", "start" -> OffsetRequest.EarliestTime)
  val jsonMessages = new ArrayBuffer[String]()

  val topics: Map[String, Map[String, Any]] = Map(
    "testJson" -> avroTopic,
    "testAvro" -> jsonTopic
  )
  val schema = new Schema.Parser parse Source.fromInputStream(getClass.getResourceAsStream("/schema.avsc")).mkString

  val bootstrapServers = "localhost:5001"

  val zkConnect = "localhost:5000"

  override def beforeAll() {
    EmbeddedKafka.start()

    EmbeddedKafka.createCustomTopic("testJson")
    EmbeddedKafka.createCustomTopic("testAvro")
    EmbeddedKafka.createCustomTopic("__consumer_offsets")
    val cfg = implicitly[EmbeddedKafkaConfig]

    val reader = new GenericDatumReader[Object](schema)
    for (i <- 0 to 10) {
      val json = s"""{"no": "$i","value":"hello"}"""
      jsonMessages += json

      EmbeddedKafka.publishStringMessageToKafka("testJson", json)
      val in = new ByteArrayInputStream(json.getBytes)
      val din = new DataInputStream(in)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      val rec = reader.read(null, decoder).asInstanceOf[GenericRecord]
      EmbeddedKafka.publishToKafka("testAvro", rec)(cfg, new TestKafkaAvroSerializer)
    }
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