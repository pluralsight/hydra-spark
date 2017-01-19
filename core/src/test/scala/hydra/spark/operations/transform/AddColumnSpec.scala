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

package hydra.spark.operations.transform

import hydra.spark.testutils.StaticJsonSource
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterEach, FunSpecLike, Inside, Matchers }

/**
 * Created by alexsilva on 8/12/16.
 */
class AddColumnSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterEach {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hydra")
    .set("spark.ui.enabled", "false")
    .set("spark.local.dir", "/tmp")
    .set("spark.driver.allowMultipleContexts", "false")

  var sparkCtx: Option[SparkContext] = None

  override def beforeEach() = {
    sparkCtx = Some(new SparkContext(sparkConf))
  }

  override def afterEach() = {
    sparkCtx.foreach(_.stop())
  }

  describe("It should add  a column") {
    it("Should add a column") {
      val sqlContext = new SQLContext(sparkCtx.get)
      val df = AddColumn("newColum", 10).transform(StaticJsonSource.createDF(sqlContext))
      df.first().getInt(3) shouldBe 10

      val df1 = AddColumn("newColum", "new").transform(StaticJsonSource.createDF(sqlContext))
      df1.first().getString(3) shouldBe "new"
    }

    it("Should add a dynamic column") {
      val sqlContext = new SQLContext(sparkCtx.get)
      val df = AddColumn("newColum", "${10+20}").transform(StaticJsonSource.createDF(sqlContext))
      df.first().getInt(3) shouldBe 30

      val df1 = AddColumn("newColum", "${T(java.lang.String).valueOf(1)}")
        .transform(StaticJsonSource.createDF(sqlContext))
      df1.first().getString(3) shouldBe "1"
    }

  }
}
