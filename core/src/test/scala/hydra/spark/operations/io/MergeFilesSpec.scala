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
import org.apache.commons.io.FileUtils
import org.scalatest.{ FunSpecLike, Inside, Matchers }

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by alexsilva on 7/26/16.
 */
class MergeFilesSpec extends Matchers with FunSpecLike with Inside {

  describe("When Merging files in a directory") {
    it("Should be configured properly") {

      inside(MergeFiles(ConfigFactory.empty()).validate) { case Invalid(errors) => errors.size shouldBe 1 }

      val props = Map("source-directory" -> "shouldntexist")

      inside(MergeFiles(ConfigFactory.parseMap(props)).validate) { case Invalid(errors) => errors.size shouldBe 1 }

      val file = java.io.File.createTempFile("json", "test")

      inside(MergeFiles(ConfigFactory.parseMap(Map("source-directory" -> file.getAbsolutePath))).validate) {
        case Invalid(errors) => errors.size shouldBe 1
      }
    }

    it("Should merge") {

      val f = new File(Thread.currentThread().getContextClassLoader.getResource("merge-files-test.json").getFile)

      val directory = Files.createTempDir()
      directory.deleteOnExit()

      val target = new File(directory, "target.file")

      FileUtils.copyFile(f, new File(directory, "1"))
      FileUtils.copyFile(f, new File(directory, "2"))

      val props = Map("source-directory" -> directory.getAbsolutePath(), "destination-file" -> target.getAbsolutePath)

      val t = MergeFiles(ConfigFactory.parseMap(props))

      //hack
      t.transform(null)

      target.exists() shouldBe true

      val merged = Source.fromFile(target.getAbsolutePath()).mkString
      val original = Source.fromFile(Thread.currentThread().getContextClassLoader.getResource("merge-files-test.json")
        .getFile).mkString

      original + original shouldBe merged
    }
  }
}

