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

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.api.{DFOperation, ValidationResult}
import hydra.spark.operations.common.{ColumnMapping, TableMapping}
import org.apache.spark.sql.DataFrame

/**
  * Created by alexsilva on 1/4/17.
  */
case class HiveTable(table: String, properties: Map[String, String], columns: Seq[ColumnMapping])
  extends DFOperation {

  val mapping = TableMapping(None, columns)

  override def transform(df: DataFrame): DataFrame = {
    val targetDf = mapping.targetDF(df)
    val hiveCtx = targetDf.sqlContext
    val hiveDf = hiveCtx.createDataFrame(targetDf.rdd, targetDf.schema)
    hiveDf.createOrReplaceTempView(s"${table}_tmp")
    hiveCtx.sql(s"create table $table as select * from ${table}_tmp")
    hiveDf
  }

  override def validate: ValidationResult = checkRequiredParams(Seq("Hive table" -> table))
}


object HiveTable {
  def apply(cfg: Config): HiveTable = {
    import configs.syntax._
    import hydra.spark.configs._
    import hydra.spark.util.DataTypes._
    def mapping(cfg: Config): ColumnMapping = {
      val name = cfg.get[String]("name").valueOrThrow(_ => new IllegalArgumentException("A column name is required."))
      val source = cfg.get[String]("source").valueOrElse(name)
      val theType = cfg.get[String]("type").valueOrElse("string")
      ColumnMapping(source, name, theType)
    }

    val properties = cfg.get[Config]("properties").valueOrElse(ConfigFactory.empty).to[Map[String, String]]
    val table = cfg.get[String]("table").valueOrElse(throw new IllegalArgumentException("Table is required for Hive"))
    val columns = cfg.get[List[Config]]("columns").valueOrElse(Seq.empty).map(mapping)

    HiveTable(table, properties, columns)
  }
}