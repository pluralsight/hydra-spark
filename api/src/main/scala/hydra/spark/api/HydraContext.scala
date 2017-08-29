/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.api

import java.lang.reflect.Constructor
import java.util.concurrent.atomic.AtomicReference
import java.util.{Properties, UUID}

import com.typesafe.config.Config
import hydra.spark.configs.ConfigSupport
import hydra.spark.events.{AsyncListenerBus, HydraListener}
import hydra.spark.internal.Logging
import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.language.implicitConversions

class HydraContext private(@transient val sc: SparkContext, config: Config) extends Logging {
  self =>

  val startTime = System.currentTimeMillis()

  private[hydra] val listenerBus = new AsyncListenerBus(config)

  private var _listenerBusStarted: Boolean = false

  val sparkUser = getCurrentUserName(sc.getConf)

  // Thread Local variable that can be used by users to pass information down the stack
  protected[hydra] val properties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      SerializationUtils.clone(parent)
    }

    override protected def initialValue(): Properties = new Properties()
  }

  setupAndStartListenerBus()

  private[api] def getCurrentUserName(conf:SparkConf): String = {
    Option(conf.get("spark.user", null)).orElse(Option(System.getenv("SPARK_USER")))
      .getOrElse(UserGroupInformation.getCurrentUser().getShortUserName())
  }

  private[hydra] def getProperties: Properties = properties.get()

  private[hydra] def setProperties(props: Properties) {
    properties.set(props)
  }

  def setProperty(key: String, value: String) {
    if (value == null) {
      properties.get.remove(key)
    } else {
      properties.get.setProperty(key, value)
    }
  }

  def getProperty(key: String): String =
    Option(properties.get).map(_.getProperty(key)).orNull


  def addHydraListener(listener: HydraListener) = {
    listenerBus.addListener(listener)
  }

  def getAccumulator(name: String): LongAccumulator = {
    sc.longAccumulator(name)
  }


  private def setupAndStartListenerBus(): Unit = {
    import configs.syntax._
    try {
      val listenerClassNames: Seq[String] =
        config.get[String]("hydra.listeners").valueOrElse("").split(',').map(_.trim).filter(_ != "")
      for (className <- listenerClassNames) {
        // Use reflection to find the right constructor
        val constructors = {
          val listenerClass = Class.forName(className, true, getContextOrSparkClassLoader)
          listenerClass
            .getConstructors
            .asInstanceOf[Array[Constructor[_ <: HydraListener]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        lazy val zeroArgumentConstructor = constructors.find { c =>
          c.getParameterTypes.isEmpty
        }
        val listener: HydraListener = {
          if (constructorTakingSparkConf.isDefined) {
            constructorTakingSparkConf.get.newInstance(config)
          } else if (zeroArgumentConstructor.isDefined) {
            zeroArgumentConstructor.get.newInstance()
          } else {
            throw new SparkException(
              s"$className did not have a zero-argument constructor or a" +
                " single-argument constructor that accepts SparkConf. Note: if the class is" +
                " defined inside of another Scala class, then its constructors may accept an" +
                " implicit parameter that references the enclosing class; in this case, you must" +
                " define the listener as a top-level class in order to prevent this extra" +
                " parameter from breaking Spark's ability to find a valid constructor.")
          }
        }
        listenerBus.addListener(listener)
        log.info(s"Registered listener $className")
      }
    } catch {
      case e: Exception =>
        try {
        } finally {
          throw new HydraException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start(sc)
    _listenerBusStarted = true
  }

  private def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(SparkContext.getClass.getClassLoader)

}

object HydraContext extends Logging {

  import configs.syntax._

  def builder(): Builder = new Builder

  class Builder extends ConfigSupport {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var sparkConf = new SparkConf()

    private[this] var userSparkContext: Option[SparkContext] = None

    private[api] def sparkContext(ctx: SparkContext): Builder = synchronized {
      userSparkContext = Option(ctx)
      this
    }

    def setConfig(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    def setConfig(conf: Config): Builder = synchronized {
      val name = conf.get[String]("name").valueOrElse(UUID.randomUUID().toString)
      sparkConf = sparkConf(conf, name)
      this
    }


    /** The active HydraContext for the current thread. */
    private val activeHydraContext = new InheritableThreadLocal[HydraContext]

    /** Reference to the root HydraContext. */
    private val defaultContext = new AtomicReference[HydraContext]


    def getOrCreate(): HydraContext = synchronized {
      var ctx = activeHydraContext.get()
      if ((ctx ne null) && !ctx.sc.isStopped) {
        if (!config.isEmpty) {
          log.warn("Using an existing HydraContext; some configuration may not take effect.")
        }
        return ctx
      }

      HydraContext.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        ctx = defaultContext.get()
        if ((ctx ne null) && !ctx.sc.isStopped) {
          if (!config.isEmpty) {
            log.warn("Using an existing HydraContext; some configuration may not take effect.")
          }

          return ctx
        }

        // No active nor global default context. Create a new one.
        val theContext = userSparkContext.getOrElse {
          val builder = options.foldLeft(SparkSession.builder()) { (b, o) => b.config(o._1, o._2) }
          val spark = builder.config(sparkConf).getOrCreate()
          spark.sparkContext
        }
        ctx = new HydraContext(theContext, config)
        defaultContext.set(ctx)

        return ctx
      }
    }
  }

}










