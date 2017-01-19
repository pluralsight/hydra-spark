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

package hydra.spark.kafka.types

import org.springframework.util.ClassUtils

/**
 * Created by alexsilva on 10/30/15.
 *
 * Generic trait that defines a message that is to be persisted in Kafka.
 * In Hydra, message types are bound to a specific Kafka producer type.
 * This can be overriden by adding another entry in hydra.common.kafka.producers config object.
 * See full documentation for detauls.
 *
 * @tparam K The message key type
 * @tparam P The message payload type
 */
trait KafkaMessage[+K, +P] {

  val timestamp = System.currentTimeMillis

  def key: K

  def payload: P

  /**
   * A "friendly" identifier that should identify the type of this message.
   * Defaults to a substring equal to the class name up to the word 'Message' (if it exists.)
   * If the word 'Message' is not part of the class name, the entire class name (including package) will be used.
   * For instance: AvroMessage -> 'avro'; JsonMessage ->'json'; SomeOther -> com.package.SomeOther
   * CAUTION: It needs to be unique.
   *
   * @return
   */
  def typeName: String = {
    val cname = ClassUtils.getShortName(getClass)
    val idx = cname.indexOf("Message")
    if (idx != -1)
      cname.take(idx).toLowerCase
    else
      getClass.getName
  }
}
