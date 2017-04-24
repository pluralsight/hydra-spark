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

import java.io.File
import java.nio.file.Files

import hydra.spark.testutils.{LocalSparkContext, StaticJsonSource}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}
import spray.json._

/**
  * Created by alexsilva on 1/4/17.
  */
class HiveTableSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with DefaultJsonProtocol
  with BeforeAndAfterAll {

  val warehouseDir: File = makeWarehouseDir()

  var hive: SparkSession = _

  override def beforeAll() = {

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("hydra-spark-hive-test")
      .set("spark.ui.enabled", "false")
      .set("spark.local.dir", "/tmp")
      .set("spark.sql.warehouse.dir", warehouseDir.toURI.getPath)

    //prevent spark context from being created, because we need to add hive support
    hive = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  }

  override def afterAll() = {
    //prevent spark context from being created, because we need to add hive support
    LocalSparkContext.stop(hive.sparkContext)
    warehouseDir.delete()
    FileUtils.deleteDirectory(new File("metastore_db"))
  }


  describe("When writing to Hive") {

    it("should save") {


      val df = StaticJsonSource.createDF(hive.sqlContext)
      HiveTable("my_table", Map("option.path" -> warehouseDir.toURI.getPath), Seq.empty).transform(df)
      hive.catalog.refreshTable("my_table")
      val dfHive = hive.sql("SELECT * from my_table")
      val hiveDf = dfHive.toJSON.collect()
        .map(_.parseJson.asJsObject.fields.filter(!_._1.startsWith("data"))).map(JsObject(_))
      val datelessDf = df.toJSON.collect()
        .map(_.parseJson.asJsObject.fields.filter(!_._1.startsWith("data"))).map(JsObject(_))
      datelessDf.foreach { json =>
        hiveDf should contain(json)
      }
    }
  }

  def makeWarehouseDir(): File = {
    val warehouseDir = Files.createTempDirectory("_hydra").toFile
    warehouseDir
  }
}
