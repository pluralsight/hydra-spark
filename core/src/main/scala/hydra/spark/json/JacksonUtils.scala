package hydra.spark.json

import org.apache.spark.sql.types._

/**
  * From org.apache.spark.sql.catalyst.json.JacksonUtils (Spark 2.1.0)
  *
  * Created by alexsilva on 1/25/17.
  */
object JacksonUtils {

  def verifySchema(schema: StructType): Unit = {
    def verifyType(name: String, dataType: DataType): Unit = dataType match {
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | StringType | TimestampType | DateType | BinaryType | _: DecimalType =>

      case st: StructType => st.foreach(field => verifyType(field.name, field.dataType))

      case at: ArrayType => verifyType(name, at.elementType)

      case mt: MapType => verifyType(name, mt.keyType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert column $name of type ${dataType.simpleString} to JSON.")
    }

    schema.foreach(field => verifyType(field.name, field.dataType))
  }
}

