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
import hydra.spark.api.Invalid
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.testutils.{ListOperation, SharedSparkContext, StaticJsonSource}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Inside, Matchers}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by alexsilva on 6/22/16.
  */
class SaveAsJsonSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterEach with SharedSparkContext {

  describe("When Saving as JSON") {

    it("Should be configured properly") {

      intercept[IllegalArgumentException] {
        SaveAsJson(ConfigFactory.empty())
      }

      val file = java.io.File.createTempFile("json", "test")
      inside(SaveAsJson(ConfigFactory.parseMap(Map("directory" -> file.getAbsolutePath, "overwrite" -> true))).validate) {
        case Invalid(errors) => errors.size shouldBe 1
      }
    }

    it("Should create specified subdirectory by default") {
      import spray.json._

      val tmpDir = Files.createTempDir()
      val path = tmpDir.getAbsolutePath + "/test3/mysubdir"

      val props = ConfigFactory.parseString(
        s"""
           |directory = $path
           |overwrite = false
        """.stripMargin
      )

      val t = SaveAsJson(props)

      val sbd = SparkBatchDispatch("test1", StaticJsonSource, Seq(t), props)

      sbd.run()

      val output = new File(t.directory)

      val files = sbd.sparkSession.sparkContext.wholeTextFiles(output.getAbsolutePath, 1)
      val l = mutable.ListBuffer[JsValue]()
      files.collect().foreach(s => s._2.split("\\n").foreach(r => l += r.parseJson))
      l should contain theSameElementsAs StaticJsonSource.msgs.map(_.parseJson)
    }

    it("Should save with overwrite if true") {
      import spray.json._

      val tmpDir = Files.createTempDir()

      val props = ConfigFactory.parseString(
        s"""
           |directory = ${tmpDir.getAbsolutePath}
           |overwrite = true
        """.stripMargin
      )

      val t = SaveAsJson(props)

      val sbd = SparkBatchDispatch("test2", StaticJsonSource, Seq(t), props)

      sbd.run()

      val output = new File(t.directory)

      val files = sbd.sparkSession.sparkContext.wholeTextFiles(output.getAbsolutePath, 1)
      val l = mutable.ListBuffer[JsValue]()
      files.collect().foreach(s => s._2.split("\\n").foreach(r => l += r.parseJson))
      l should contain theSameElementsAs StaticJsonSource.msgs.map(_.parseJson)
    }

    it("Should create timestamp subdirectory if current_timestamp specified") {
        import spray.json._

        val tmpDir = Files.createTempDir()

        val props = ConfigFactory.parseString(
          s"""
             |directory = s"${tmpDir.getAbsolutePath}/#{current_timestamp()}"
             |overwrite = false
        """.stripMargin
        )

        val t = SaveAsJson(props)

        val sbd = SparkBatchDispatch("test3", StaticJsonSource, Seq(t), props)

        sbd.run()

        //find created sub-directory to compare files in subdir to input
        val dir = t.directory.replace("#{current_timestamp()}","")
        val dir_list = new File(dir).listFiles()
        val subdir = dir_list.filter(_.isDirectory)(0)


        val files = sbd.sparkSession.sparkContext.wholeTextFiles(subdir.getAbsolutePath, 1)
        val l = mutable.ListBuffer[JsValue]()
        files.collect().foreach(s => s._2.split("\\n").foreach(r => l += r.parseJson))
        l should contain theSameElementsAs StaticJsonSource.msgs.map(_.parseJson)
      }

    it("Should save as codec specified") {
      import spray.json._

      val tmpDir2 = Files.createTempDir()

      val props = ConfigFactory.parseString(
        s"""
           |directory = "${tmpDir2.getAbsolutePath}"
           |codec = gzip
           |overwrite = true
        """.stripMargin
      )

      val t = SaveAsJson(props)

      val sbd = SparkBatchDispatch("test4", StaticJsonSource, Seq(t), props)

      sbd.run()

      val output = new File(t.directory)

      val files = sbd.sparkSession.sparkContext.wholeTextFiles(output.getAbsolutePath, 1)
      val l = mutable.ListBuffer[JsValue]()
      files.collect().foreach(s => s._2.split("\\n").foreach(r => l += r.parseJson))
      l should contain theSameElementsAs StaticJsonSource.msgs.map(_.parseJson)
    }


  }

  override def beforeEach = ListOperation.reset
}

