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

import hydra.spark.api.Invalid
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
 * Created by alexsilva on 6/2/16.
 */
class JsonFileSourceSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
    with Eventually with BeforeAndAfterAll with SharedSparkContext {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hydra")
    .set("spark.ui.enabled", "false")
    .set("spark.local.dir", "/tmp")
    .set("spark.driver.allowMultipleContexts", "false")

  var ctx: SQLContext = _

  var ssc: StreamingContext = _

  override def beforeAll() = {
    super.beforeAll()
    ssc = new StreamingContext(sc, Durations.seconds(10))
    ctx = new SQLContext(sc)
  }

  override def afterAll() = {
    super.afterAll()
    ssc.stop(false, true)
  }

  describe("When using the file source") {
    it("Should use windowing and side when provided by the client") {
      val src = JsonFileSource("path", Map("streaming.window.duration" -> "2s", "streaming.slide.duration" -> "20s"))
      src.slideDuration.get shouldBe Durations.seconds(20)
      src.windowDuration.get shouldBe Durations.seconds(2)
    }
    it("Should use fall back to defaults when no window and slide are provided") {
      val src = JsonFileSource("path", Map.empty)
      src.slideDuration shouldBe None
      src.windowDuration shouldBe None

      JsonFileSource("path", Map.empty).createStream(ssc).slideDuration shouldBe Durations.seconds(10)
    }

    it("Should support streaming") {
      JsonFileSource("path", Map.empty).createStream(ssc)
    }

    it("Should be invalid when no path is specified") {
      val s3 = JsonFileSource("", Map.empty)
      val validation = s3.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should create a DF") {
      val file = Thread.currentThread().getContextClassLoader.getResource("json-source.json")
      val jsonSource = JsonFileSource(file.getFile, Map.empty)
      val df = jsonSource.createDF(ctx)
      df.show()
      val row = df.first()
      row.getAs[String]("type") shouldBe "status"
    }

    it("Should be parseable") {
      val dsl =
        """
          transport {
          |  version = 1
          |  spark.master = "local[*]"
          |  spark.ui.enabled = false
          |
          |source {
          |  json-file {
          |    path = "s3n://path"
          |    properties {
          |      fs.s3n.awsAccessKeyId = key
          |      fs.s3n.awsSecretAccessKey = secret
          |    }
          |  }
          |}
          |operations {
          |  publish-to-kafka {
          |    topic = topic-name
          |    properties {
          |      producer.type = async
          |    }
          |  }
          |}
          |}
          |
        """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl)

      val csv = dispatch.source.asInstanceOf[JsonFileSource]
      csv.path shouldBe "s3n://path"
      csv.properties shouldBe Map("fs.s3n.awsAccessKeyId" -> "key", "fs.s3n.awsSecretAccessKey" -> "secret")
    }

  }

  describe("When using S3 sources") {

    it("Should have the right S3 properties set") {
      val fakePath = Thread.currentThread().getContextClassLoader.getResource("profile.csv").getFile
      val s3 = JsonFileSource(
        fakePath,
        Map("fs.s3n.awsAccessKeyId" -> "key", "fs.s3n.awsSecretAccessKey" -> "secret")
      )
      s3.createDF(ctx)
      sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId") shouldBe "key"
      sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey") shouldBe "secret"
    }
  }
}

