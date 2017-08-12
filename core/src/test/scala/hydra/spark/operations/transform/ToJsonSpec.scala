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

import java.io.File

import hydra.spark.testutils.SharedSparkContext
import org.scalatest.{FunSpecLike, Matchers}

import scala.io.Source

/**
  * Created by alexsilva on 8/12/16.
  */
class ToJsonSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("When converting columns to json") {
    it("works with complex types") {
      import spray.json._
      val sqlContext = ss.sqlContext
      val path = Thread.currentThread().getContextClassLoader.getResource("data.txt").getFile
      val data = Source.fromFile(new File(path)).getLines().toArray.map(_.parseJson.asJsObject.fields("batters"))
      val df = sqlContext.read.json(path).repartition(1)
      val ndf = ToJson(Seq("batters")).transform(df)
      ndf.rdd.zipWithIndex().collect().foreach { case (row, idx) =>
        data(idx.toInt) shouldBe (row.getString(0).parseJson)
      }
    }
    it("keeps all non-json columns") {
      import spray.json._
      val sqlContext = ss.sqlContext
      val path = Thread.currentThread().getContextClassLoader.getResource("data.txt").getFile
      val data = Source.fromFile(new File(path)).getLines().toArray.map(_.parseJson.asJsObject.fields("batters"))
      val df = sqlContext.read.json(path).repartition(1)
      val ndf = ToJson(Seq("batters")).transform(df)
      ndf.columns should contain allOf("batters", "id", "name", "ppu", "topping", "type")
    }
    it("works with arrays of json") {
    import spray.json._

      val sqlContext = ss.sqlContext
    val path = Thread.currentThread().getContextClassLoader.getResource("data.txt").getFile
    val data = Source.fromFile(new File(path)).getLines().toArray.map(_.parseJson.asJsObject.fields("batters"))
    val df = sqlContext.read.json(path).repartition(1)
    val ndf = ToJson(Seq("topping")).transform(df)
    ndf.columns should contain allOf("batters", "id", "name", "ppu", "topping", "type")
  }
    it("does not fail with empty dataset") {
      val rdd = sc.parallelize("" :: Nil)
      val df = ss.sqlContext.read.json(rdd)
      val ndf = ToJson(Seq("batters")).transform(df)
      ndf.collect() shouldBe empty
    }
  }
}
