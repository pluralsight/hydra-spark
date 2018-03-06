package hydra.spark.operations.hadoop

import hydra.spark.api.{DFOperation, ValidationResult}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

case class WriteToHBase(catalog: String) extends DFOperation {

  //Try to build the catalog from dataframe?

  override def transform(df: DataFrame): DataFrame = {
    df.write
      .options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    df
  }

  override def validate: ValidationResult =
    checkRequiredParams(Seq(("HBase catalog", catalog)))
}

/**
  * *
  * def catalog = s"""{
  * |"table":{"namespace":"default", "name":"table1"},
  * |"rowkey":"key",
  * |"columns":{
  * |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
  * |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
  * |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
  * |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
  * |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
  * |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
  * |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
  * |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
  * |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
  * |}
  * |}""".stripMargin
  */