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

package hydra.spark.sources

import hydra.spark.api.{ Invalid, InvalidDslException }
import hydra.spark.dispatch.SparkDispatch
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration, ScalaFutures }
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }

/**
 * Created by alexsilva on 6/2/16.
 */
class CsvSourceSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
    with Eventually with SharedSparkContext with BeforeAndAfterAll {

  lazy val ssc = new StreamingContext(sc, Seconds(1))

  describe("When using csv sources") {

    it("Should be invalid when no path is specified") {
      val csv = CsvSource("", true)
      val validation = csv.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }
    it("Should not support streaming") {
      intercept[InvalidDslException] {
        CsvSource("path", true).createStream(ssc)
      }
    }
    it("Should infer the schema for batch jobs") {
      val path = Thread.currentThread().getContextClassLoader.getResource("profile.csv").getFile
      val csv = CsvSource(path, true)

      val df = csv.createDF(new SQLContext(sc))

      df.count() shouldBe 37
      val row = df.first()
      row.getBoolean(0) shouldBe true
      df.show()
    }

    it("Should be parseable") {
      val dsl =
        """
          |{
          |	"dispatch": {
          |		"version": 1,
          |		"spark.master": "local[*]",
          |		"spark.ui.enabled": false,
          |
          |	"source": {
          |		"csv": {
          |			"path": "path",
          |     "header": false
          |		}
          |	},
          |	"operations": {
          |		"publish-to-kafka": {
          |			"topic": "topic",
          |			"properties": {
          |				"producer.type": "async"
          |			}
          |		}
          |	}
          |}
          |}
        """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl)

      val csv = dispatch.source.asInstanceOf[CsvSource]
      csv.header shouldBe false
      csv.path shouldBe "path"
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    ssc.stop(false, true)
  }
}

