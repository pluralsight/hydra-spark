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

package hydra.spark.dispatch.context

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.scalatest.{ FunSpecLike, Matchers }

import scala.util.Try

/**
 * Created by alexsilva on 1/9/17.
 */
class DefaultSparkContextFactorySpec extends Matchers with FunSpecLike {

  val hadoopConfig =
    """
      |hadoop {
      |  fs.s3n.awsAccessKeyId = key
      |  fs.s3n.awsSecretAccessKey = secret
      |}
    """.stripMargin

  val sparkConf = new SparkConf(false).setMaster("local[*]").setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  describe("When creating a default spark context") {
    it("uses Hadoop properties") {
      Try(new DefaultSparkContextFactory().makeContext(sparkConf, ConfigFactory.parseString(hadoopConfig)))
        .map { ctx =>
          ctx.hadoopConfiguration.get("fs.s3n.awsAccessKeyId") shouldBe "key"
          ctx.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey") shouldBe "secret"
          ctx
        }.map(_.stop()).get
    }
  }
}
