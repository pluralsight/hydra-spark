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
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigList, ConfigObject, ConfigUtil}
import hydra.spark.util.Quote

import scala.concurrent.duration.FiniteDuration

/**
  * Created by alexsilva on 1/3/17.
  */
package object configs {

  trait ConfigLike[T] extends Serializable {
    def get(cfg: Config, key: String): Option[T]
  }

  object ConfigLike {

    implicit object ConfigLikeString extends ConfigLike[String] {
      override def get(cfg: Config, key: String): Option[String] = {
        if (cfg.hasPath(key)) Some(cfg.getString(key)) else None
      }
    }

    implicit object ConfigLikeBoolean extends ConfigLike[Boolean] {
      override def get(cfg: Config, key: String): Option[Boolean] = {
        if (cfg.hasPath(key)) Some(cfg.getBoolean(key)) else None
      }
    }

    implicit object ConfigLikeConfig extends ConfigLike[Config] {
      override def get(cfg: Config, key: String): Option[Config] = {
        if (cfg.hasPath(key)) Some(cfg.getConfig(key)) else None
      }
    }

    implicit object ConfigLikeInt extends ConfigLike[Int] {
      override def get(cfg: Config, key: String): Option[Int] = {
        if (cfg.hasPath(key)) Some(cfg.getInt(key)) else None
      }
    }

    implicit object ConfigLikeProperties extends ConfigLike[Properties] {
      override def get(cfg: Config, key: String): Option[Properties] = {
        import scala.collection.JavaConversions._
        if (cfg.hasPath(key)) {
          val props = new Properties()

          val map: Map[String, Object] = cfg.entrySet().map({ entry =>
            entry.getKey -> entry.getValue.unwrapped()
          })(collection.breakOut)

          props.putAll(map)
          Some(props)
        } else None
      }
    }

    implicit object ConfigLikeMap extends ConfigLike[Map[String, String]] {
      override def get(cfg: Config, key: String): Option[Map[String, String]] = {
        import scala.collection.JavaConversions._
        if (cfg.hasPath(key)) {
          val map: Map[String, String] = cfg.getConfig(key).entrySet().map(entry =>
            Quote.unquote(entry.getKey) -> entry.getValue.unwrapped().toString)(collection.breakOut)
          Some(map)
        } else None
      }
    }

    implicit object ConfigLikeConfigObject extends ConfigLike[ConfigObject] {
      override def get(cfg: Config, key: String): Option[ConfigObject] = {
        if (cfg.hasPath(key)) Some(cfg.getObject(key)) else None
      }
    }

    implicit object ConfigLikeList extends ConfigLike[List[String]] {
      override def get(cfg: Config, key: String): Option[List[String]] = {
        import scala.collection.JavaConversions._
        if (cfg.hasPath(key)) Some(cfg.getStringList(key).toList) else None
      }
    }

    implicit object ConfigLikeListConfig extends ConfigLike[List[Config]] {
      override def get(cfg: Config, key: String): Option[List[Config]] = {
        import scala.collection.JavaConversions._
        if (cfg.hasPath(key)) Some(cfg.getConfigList(key).toList) else None
      }
    }

    implicit object ConfigLikeListConfigObject extends ConfigLike[List[ConfigObject]] {
      override def get(cfg: Config, key: String): Option[List[ConfigObject]] = {
        import scala.collection.JavaConversions._
        if (cfg.hasPath(key)) Some(cfg.getObjectList(key).toList) else None
      }
    }

    implicit object ConfigLikeFiniteDuration extends ConfigLike[FiniteDuration] {
      override def get(cfg: Config, key: String): Option[FiniteDuration] = {
        val inSeconds = TimeUnit.SECONDS
        if (cfg.hasPath(key)) Some(new FiniteDuration(cfg.getDuration(key, inSeconds), inSeconds)) else None
      }
    }

    implicit object ConfigLikeConfigList extends ConfigLike[ConfigList] {
      override def get(cfg: Config, key: String): Option[ConfigList] = {
        if (cfg.hasPath(key)) Some(cfg.getList(key)) else None
      }
    }

  }

  implicit class ConfigConversionsPimp(cfg: Config) {
    def get[T](key: String)(implicit cl: ConfigLike[T]) = cl.get(cfg, key)

    def flattenAtKey(key: String): Map[String, String] = {
      import scala.collection.JavaConverters._
      Map(cfg.entrySet().asScala
        .filter(k => ConfigUtil.splitPath(k.getKey()).asScala.head.startsWith(key))
        .map(e => ConfigUtil.splitPath(e.getKey()).asScala.mkString(".") -> e.getValue.unwrapped.toString).toSeq: _*)
    }
  }

}
