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

import com.typesafe.config.Config
import hydra.spark.configs._
import hydra.spark.api._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FileUtil, Path }
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }

/**
 * Created by alexsilva on 7/26/16.
 */
class MergeFiles(source: String, targetFile: String, deleteSource: Boolean = false) extends DFOperation {

  val hadoopConfig = new Configuration()

  override def id: String = s"merge-files:$source"

  override def transform(df: DataFrame): DataFrame = {
    val fs = FileSystem.get(new File(source).toURI, hadoopConfig)
    FileUtil.copyMerge(fs, new Path(source), fs, new Path(targetFile), deleteSource, hadoopConfig, null)
    df
  }

  override def validate: ValidationResult = {
    val v = for {
      dir <- validateSource
      target <- validateTarget
    } yield dir

    v match {
      case Success(x) => Valid
      case Failure(ex) => Invalid("merge-files", ex.getMessage)
    }
  }

  private def validateSource: Try[File] = {
    val dir = new File(source)
    if (dir.isDirectory) Success(dir) else Failure(new IllegalArgumentException(s"$source is not a directory."))
  }

  private def validateTarget: Try[File] = {
    val t = new File(targetFile)
    if (t.exists()) Success(t) else Failure(new IllegalArgumentException(s"$targetFile exists."))
  }

}

object MergeFiles {
  def apply(cfg: Config): MergeFiles = {
    import configs.syntax._
    val source = cfg.get[String]("source-directory").valueOrElse("")
    val target = cfg.get[String]("destination-file").valueOrElse("")
    val deleteSource = cfg.get[Boolean]("delete-source").valueOrElse(false)
    new MergeFiles(source, target, deleteSource)
  }
}