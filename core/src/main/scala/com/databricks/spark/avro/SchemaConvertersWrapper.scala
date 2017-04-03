package com.databricks.spark.avro

import hydra.spark.avro.SchemaRegistrySupport
import org.apache.spark.sql.types.DataType

/**
  * Created by alexsilva on 4/3/17.
  */
object SchemaConvertersWrapper {

  def convert(schema: String, props: Map[String, String]): DataType = {
    val schemaResolver = new SchemaRegistrySupport {
      override val properties: Map[String, String] = props
    }
    SchemaConverters.toSqlType(schemaResolver.getValueSchema(schema)).dataType
  }
}
