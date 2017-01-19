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

package hydra.spark.operations.io

import java.io.File

import com.google.common.io.Files
import com.typesafe.config.ConfigFactory
import hydra.spark.api.{ Invalid, Operations }
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.testutils.{ ListOperation, SharedSparkContext, StaticJsonSource }
import org.scalatest.{ BeforeAndAfterEach, FunSpecLike, Inside, Matchers }

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by alexsilva on 6/22/16.
 */
class SaveAsJsonSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterEach with SharedSparkContext {

  describe("When Saving as JSON") {
    it("Should be configured properly") {

      inside(SaveAsJson(ConfigFactory.empty()).validate) { case Invalid(errors) => errors.size shouldBe 1 }

      val props = Map("directory" -> "shouldntexist")

      inside(SaveAsJson(ConfigFactory.parseMap(props)).validate) { case Invalid(errors) => errors.size shouldBe 1 }

      val file = java.io.File.createTempFile("json", "test")

      inside(SaveAsJson(ConfigFactory.parseMap(Map("directory" -> file.getAbsolutePath))).validate) {
        case Invalid(errors) => errors.size shouldBe 1
      }
    }

    it("Should save") {
      import spray.json._

      val file = Files.createTempDir()

      val props = ConfigFactory.parseString(
        s"""
           |spark.master = "local[4]"
           |spark.ui.enabled = false
           |spark.driver.allowMultipleContexts = false
           |directory = ${file.getAbsolutePath()}
        """.stripMargin
      )

      val t = SaveAsJson(props)

      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(t), props, scl)

      sd.run()

      val output = new File(t.output.toString)

      val files = sd.ctx.sparkContext.wholeTextFiles(output.getAbsolutePath, 1)
      val l = mutable.ListBuffer[JsValue]()
      files.collect().foreach(s => s._2.split("\\n").foreach(r => l += r.parseJson))
      l should contain theSameElementsAs StaticJsonSource.msgs.map(_.parseJson)

      new File(t.output.toString).delete()

      sd.stop()
    }
  }

  override def beforeEach = ListOperation.reset
}

