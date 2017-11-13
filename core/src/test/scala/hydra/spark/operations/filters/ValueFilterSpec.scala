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

package hydra.spark.operations.filters

import com.typesafe.config.ConfigFactory
import hydra.spark.dispatch.SparkBatchTransformation
import hydra.spark.testutils.{ListOperation, SharedSparkContext, StaticJsonSource}
import hydra.spark.api.Source
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}
import spray.json._

/**
 * Created by alexsilva on 6/24/16.
 */
class ValueFilterSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with SharedSparkContext {

  val config = ConfigFactory.parseString(
    """
      |spark.master = "local[4]"
      |spark.ui.enabled = false
      |spark.driver.allowMultipleContexts = false
    """.stripMargin
  )
//  val email = """"email":"hydra@dataisawesome.com","""
//  val msgs = for (i <- 0 to 10)
//    yield s"""{"msg_no": $i, "timestamp": "2017-11-07 11:${("0" + i).takeRight(2)}:00"}"""
//
//  val source = Source[String]


  describe("When Filtering by a value") {
    it("Should only include matching rows for equal operation") {
      val json = StaticJsonSource.msgs(0).parseJson
      val t = ValueFilter("msg_no", 0, "=")
      SparkBatchTransformation("test", StaticJsonSource, Seq(t, ListOperation), config).run()
      ListOperation.l.size shouldBe 1
      ListOperation.l.map(_.parseJson) should contain(json)
    }
    it("Should only include matching rows for > operation") {
      val json = StaticJsonSource.msgs(3).parseJson
      val t = ValueFilter("msg_no", 2, ">")
      SparkBatchTransformation("test", StaticJsonSource, Seq(t, ListOperation), config).run()
      println(ListOperation.l)
      ListOperation.l.size shouldBe 8
      ListOperation.l.map(_.parseJson) should contain(json)
    }
    it("Should only include matching rows for >= operation") {
      val json = StaticJsonSource.msgs(2).parseJson
      val t = ValueFilter("msg_no", 2, ">=")
      SparkBatchTransformation("test", StaticJsonSource, Seq(t, ListOperation), config).run()
      println(ListOperation.l)
      ListOperation.l.size shouldBe 9
      ListOperation.l.map(_.parseJson) should contain(json)
    }
    it("Should only include matching rows for < operation") {
      val json = StaticJsonSource.msgs(1).parseJson
      val t = ValueFilter("msg_no", 2, "<")
      SparkBatchTransformation("test", StaticJsonSource, Seq(t, ListOperation), config).run()
      println(ListOperation.l)
      ListOperation.l.size shouldBe 2
      ListOperation.l.map(_.parseJson) should contain(json)
    }
    it("Should only include matching rows for <= operation") {
      val json = StaticJsonSource.msgs(2).parseJson
      val t = ValueFilter("msg_no", 2, "<=")
      SparkBatchTransformation("test", StaticJsonSource, Seq(t, ListOperation), config).run()
      println(ListOperation.l)
      ListOperation.l.size shouldBe 3
      ListOperation.l.map(_.parseJson) should contain(json)
    }
  }

  override def beforeEach() = {
    ListOperation.reset()
    super.beforeEach()
  }
}