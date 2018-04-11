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

package hydra.spark.app.factories

import java.lang.reflect.Method

import com.typesafe.config._
import hydra.spark.api.InvalidDslException
import hydra.spark.configs._
import hydra.spark.util.{CaseClassFactory, ReflectionUtils}

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.runtime.universe._


/**
  * Methods to help constructing sources and operations from config files using reflection.
  *
  * Created by alexsilva on 10/21/16.
  */
object FactoryHelper {

  val defaults = ConfigFactory.defaultReference.withFallback(ConfigFactory.load(getClass.getClassLoader, "reference"))

  /**
    *
    * @param elm     The config object for the element to be materialized.
    * @param choices The pool of choices to pick from
    * @param props   The set of 'global' properties that can be used in the materialized objects using the format
    *                ${prop.name}
    * @tparam T
    * @return
    */
  def materialize[T: TypeTag](matType: String, elm: ConfigObject, choices: Map[String, Class[_ <: T]],
                              props: Config): Seq[T] = {
    val elements = elm.entrySet().asScala.map(x => {
      val dk = DslKey(x.getKey)
      //try to look up in the map, if not found try to load it as a class name
      val c = scala.util.Try(choices.get(dk.kname).getOrElse(Class.forName(dk.kname).asInstanceOf[Class[_ <: T]]))
      if (c.isFailure)
        throw InvalidDslException(s"'${dk.kname}' is not a known $matType. Possible choices: ${choices.keys.mkString(",")}")
      val elemCfg = x.getValue.atPath(dk.kname).getConfig(dk.kname).resolveWith(props)
      dk -> scala.util.Try(instantiate(c.get, elemCfg))
    }).toSeq

    elements.sortWith(_._1.order < _._1.order).map(_._2.get)
  }

  private def instantiate[T: TypeTag](clazz: Class[T], cfg: Config): T = {
    import configs.syntax._
    val cname = s"""hydra.spark.defaults."${clazz.getName}""""
    val defaultProps = defaults.get[ConfigObject](cname).valueOrElse(ConfigFactory.empty().root()).toConfig

    val propsCfg = ConfigFactory
      .parseMap((defaultProps.flattenAtKey("properties") ++ cfg.flattenAtKey("properties")).asJava)

    val elemCfg = cfg.withFallback(propsCfg)

    //if there is a constructor on the companion object that takes a Config object, we will use that one.
    companion(clazz) match {
      case Some(companion) => companion._2.invoke(companion._1, elemCfg).asInstanceOf[T]
      case None => {
        val factory = new CaseClassFactory[T](clazz)
        val params = factory.properties.map(props => elemCfg.getAsScala(props._1, props._2))
        factory.buildWith(params.toSeq)
      }
    }
  }

  private def companion[T](clazz: Class[T]): Option[(T, Method)] = {
    try {
      val companion = ReflectionUtils.companionOf(clazz)
      companion.getClass.getMethods.toList.filter(m => m.getName == "apply"
        && m.getParameterTypes.toList == List(classOf[Config])) match {
        case Nil => None
        case method :: Nil => Some(companion, method)
        case _ => None
      }
    } catch {
      case e: ClassNotFoundException => None
    }
  }

  implicit class ConfigPimp(cfg: Config) {

    import scala.reflect.runtime.universe._

    val m = runtimeMirror(getClass.getClassLoader)

    import configs.syntax._

    def getAsScala(key: String, tpe: TypeTag[_]) = {
      val c = tpe.tpe.typeSymbol.asClass
      val clz: Class[_] = scala.util.Try(m.runtimeClass(c))
        .recover { case e: ClassNotFoundException => classOf[AnyRef] }.get
      val v = clz match {
        case q if q == classOf[Seq[String]] => cfg.get[List[String]](key).valueOrElse(List.empty)
        case q if q == classOf[Map[_, _]] => cfg.get[Config](key).valueOrElse(ConfigFactory.empty).to[Map[String, String]]
        case q if q == classOf[String] => cfg.getString(key)
        case q if q == classOf[Int] => cfg.getInt(key)
        case q if q == classOf[Long] => cfg.getLong(key)
        case q if q == classOf[Double] => cfg.getDouble(key)
        case q if q == classOf[Boolean] => cfg.getBoolean(key)
        case q if q == classOf[Config] => cfg.getConfig(key)
        case q if q == classOf[AnyRef] => cfg.getAnyRef(key)
        case q => throw new IllegalArgumentException(s"No known conversion for property $q.")
      }

      v

    }
  }

}

private[app] case class DslKey(configKey: String) {
  val cfgKeyParts = configKey.split(":")
  val hasOrder = cfgKeyParts.size > 1
  val kname = if (hasOrder) cfgKeyParts.drop(1).mkString(":") else configKey
  val order: Int =
    if (hasOrder) scala.util.Try(cfgKeyParts(0).toInt).recover { case t: Throwable => 0 }.get else 0
}