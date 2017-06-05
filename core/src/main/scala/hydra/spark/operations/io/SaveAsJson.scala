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

import com.typesafe.config.Config
import hydra.spark.api._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by alexsilva on 6/21/16.
  */
case class SaveAsJson(directory: String, codec: Option[String], overwrite: Boolean = false) extends DFOperation {

  override def id: String = s"save-as-json-$directory-$codec"

  override def transform(df: DataFrame): DataFrame = {
    df.write.option("compression", codec.getOrElse("none"))
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists)
      .json(directory)
    df
  }

  override def validate: ValidationResult = {
    if (!directory.isEmpty) {
      val d = new Path(directory)
      val fs = d.getFileSystem(new Configuration())
      if (fs.isDirectory(d)) Valid else Invalid(ValidationError("json", s"$directory is not a directory"))
    } else Invalid(ValidationError("json", s"Directory cannot be empty."))

  }
}

object SaveAsJson {
  def apply(cfg: Config): SaveAsJson = {
    import configs.syntax._
    val directory = cfg.get[String]("directory")
      .valueOrThrow(c => new IllegalArgumentException("A directory is required."))
    val codec = cfg.get[String]("codec").toOption
    val overwrite = cfg.get[Boolean]("overwrite").valueOrElse(false)
    SaveAsJson(directory, codec, overwrite)
  }
}