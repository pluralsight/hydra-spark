package hydra.spark.sql.types

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.JsonDecoder
import org.apache.spark.sql.types.{DataType, SQLUserDefinedType, StringType, UserDefinedType}
import org.apache.spark.sql.types.DataType
import spray.json._

import scala.util.Try

/**
  * Created by dustinvannoy on 1/24/17.
  */

case class AvroJsonRecord(value: String, schema: Schema)

@SQLUserDefinedType(udt = classOf[JsonType])
class JsonType extends  UserDefinedType[AvroJsonRecord] {

  override def sqlType: DataType = StringType

  override def serialize(obj: Any): AvroJsonRecord  = {
    obj match {
      case record: GenericRecord => AvroJsonRecord(record.toString, record.getSchema)
      case other =>
        throw new IllegalArgumentException(s"Expecting a ${classOf[GenericRecord].getName}; got ${obj.getClass}.")
    }
  }

  override def userClass: Class[GenericRecord] = classOf[GenericRecord]

  override def deserialize(datum: Any): GenericRecord = {
    datum match {
      case record:AvroJsonRecord =>
        val reader = new GenericDatumReader[GenericRecord](record.schema)
        val decoder =  new JsonDecoder(record.schema, record.value)
        reader.read(null, decoder)
    }
  }
}
