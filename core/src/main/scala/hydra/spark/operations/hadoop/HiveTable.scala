/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.operations.hadoop

import com.typesafe.config.Config
import hydra.spark.api.{ DFOperation, ValidationResult }
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import org.apache.spark.sql.{ AnalysisException, Column, DataFrame, SaveMode }

import scala.util.Try

/**
 * Created by alexsilva on 1/4/17.
 */
case class HiveTable(table: String, properties: Map[String, String], columns: Seq[HiveColumnMapping])
    extends DFOperation {

  val mapping = HiveTableMapping(columns)

  override def transform(df: DataFrame): DataFrame = {
    val targetDf = mapping.targetDf(df)
    val ctx = targetDf.sqlContext
    val hiveCtx = if (ctx.isInstanceOf[HiveContext]) ctx.asInstanceOf[HiveContext] else new HiveContext(ctx.sparkContext)
    val format = properties.get("format").getOrElse("parquet")
    val hiveDf = hiveCtx.createDataFrame(targetDf.rdd, targetDf.schema)
    val writer = hiveDf.write.format(format).mode(SaveMode.Append)
    val optionWriter = properties.filter(_._1.startsWith("option."))
      .foldLeft(writer)((writer, o) => writer.option(o._1.substring(7), o._2))

    optionWriter.saveAsTable(table)
    hiveDf
  }

  override def validate: ValidationResult = checkRequiredParams(Seq("Hive table" -> table))
}

case class HiveColumnMapping(source: String, target: String, `type`: DataType)

private[hadoop] case class HiveTableMapping(mapping: Seq[HiveColumnMapping]) {

  val mappingByTarget: Map[String, HiveColumnMapping] = mapping.map(m => m.target -> m).toMap

  val targetSchema: Seq[StructField] = mapping.map(m => StructField(m.target, m.`type`))

  /**
   * Converts the source data frame into the target dataframe given the mapping
   *
   * @param df
   */
  def targetDf(df: DataFrame): DataFrame = {
    val tdf = targetCols(df)
    df.select(tdf: _*)
  }

  private def targetCols(df: DataFrame): Seq[Column] = {
    val target = targetSchema.map { f =>
      val mapping = mappingByTarget(f.name)
      Try(df(mapping.source)).recover { case t: AnalysisException => lit(null) }
        .get.as(mapping.target).cast(mapping.`type`)
    }

    if (target.isEmpty) inferTargetColumns(df) else target
  }

  private def inferTargetColumns(df: DataFrame): Seq[Column] = {
    val fcols = flattenSchema(df.schema)
    val fs = fcols.map(c => df(c.toString()).as(c.toString().replace(".", "_")))
    fs
  }

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    import org.apache.spark.sql.functions._
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }
}

object HiveTable {
  def apply(cfg: Config): HiveTable = {
    import hydra.spark.configs._
    import hydra.spark.util.DataTypes._
    def mapping(cfg: Config): HiveColumnMapping = {
      val name = cfg.get[String]("name").getOrElse(throw new IllegalArgumentException("A column name is required."))
      val source = cfg.get[String]("source").getOrElse(name)
      val theType = cfg.get[String]("type").getOrElse("string")
      HiveColumnMapping(source, name, theType)
    }

    val properties = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty[String, String])
    val table = cfg.get[String]("table").getOrElse(throw new IllegalArgumentException("Table is required for Hive"))
    val columns = cfg.get[List[Config]]("columns").getOrElse(Seq.empty).map(mapping)

    HiveTable(table, properties, columns)
  }
}