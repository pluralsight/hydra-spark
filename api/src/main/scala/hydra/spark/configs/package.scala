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

package hydra.spark

import java.util.Properties

import com.typesafe.config.{Config, ConfigUtil}
import hydra.spark.util.Quote

/**
  * Created by alexsilva on 1/3/17.
  */
package object configs {

  trait ConfigConvertible[T] extends Serializable {
    def to(cfg: Config): T
  }

  object ConfigConvertible {

    implicit object ConfigLikeProperties extends ConfigConvertible[Properties] {
      override def to(cfg: Config): Properties = {
        import scala.collection.JavaConversions._
        val props = new Properties()

        val map: Map[String, Object] = cfg.entrySet().map({ entry =>
          entry.getKey -> entry.getValue.unwrapped()
        })(collection.breakOut)

        props.putAll(map)
        props
      }
    }

    implicit object ConfigLikeMap extends ConfigConvertible[Map[String, String]] {
      override def to(cfg: Config): Map[String, String] = {
        import scala.collection.JavaConverters._

        val map: Map[String, String] = cfg.entrySet().asScala.map(entry =>
          Quote.unquote(entry.getKey) -> entry.getValue.unwrapped().toString)(collection.breakOut)
        map
      }
    }

  }

  implicit class ConfigConversionsPimp(cfg: Config) {
    def to[T](implicit cl: ConfigConvertible[T]) = cl.to(cfg)

    def flattenAtKey(key: String): Map[String, String] = {
      import scala.collection.JavaConverters._
      Map(cfg.entrySet().asScala
        .filter(k => ConfigUtil.splitPath(k.getKey()).asScala.head.startsWith(key))
        .map(e => ConfigUtil.splitPath(e.getKey()).asScala.mkString(".") -> e.getValue.unwrapped.toString).toSeq: _*)
    }
  }

}
