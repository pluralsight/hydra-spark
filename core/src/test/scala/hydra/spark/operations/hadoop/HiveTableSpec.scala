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

import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spray.json._

/**
  * Created by alexsilva on 1/4/17.
  */
class HiveTableSpec extends Matchers with FunSpecLike with BeforeAndAfterAll with DefaultJsonProtocol
  with SharedSparkContext {

  val warehouseDir = makeWarehouseDir()


  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hydra-spark-hive-test")
    .set("spark.ui.enabled", "false")
    .set("spark.local.dir", "/tmp")
    .set("spark.sql.warehouse.dir", warehouseDir.toURI.getPath)

  override def beforeAll() = super.beforeAll()

  override def afterAll(): Unit = {
    super.afterAll()
    warehouseDir.delete()
    FileUtils.deleteDirectory(new File("metastore_db"))
  }

  describe("When writing to Hive") {

    val hive = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    it("should save") {
      val df = StaticJsonSource.createDF(hive.sqlContext)
      HiveTable("test", Map("option.path" -> warehouseDir.toURI.getPath), Seq.empty).transform(df)
      val dfHive = hive.sql("SELECT * from test")

      dfHive.toJSON.foreach(x=>println(x))
      val hiveDf = dfHive.toJSON.collect()
        .map(_.parseJson.asJsObject.fields.filter(!_._1.startsWith("data"))).map(new JsObject(_))
      val datelessDf = df.toJSON.collect()
        .map(_.parseJson.asJsObject.fields.filter(!_._1.startsWith("data"))).map(new JsObject(_))

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
