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

package hydra.spark.util

import hydra.spark.api.InvalidDslException
import org.apache.spark.sql.types._

/**
  * Created by alexsilva on 8/15/16.
  */
object DataTypes {

  private val arrayElemTypeRegex = """\[.*\]""".r

  implicit def nameToDataType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    name.toLowerCase match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case dataType if dataType.startsWith("array") => arrayDataType(dataType)
      case other => nonDecimalNameToType(other)
    }
  }

  private def arrayDataType(arrayType: String) = {
    arrayElemTypeRegex.findFirstIn(arrayType) match {
      case Some(arrayType) =>
        val elemType = nameToDataType(arrayType.substring(1, arrayType.length - 1))
        ArrayType(elemType, true)

      case None => throw new InvalidDslException("Arrays need to have a type, such as array[string].")
    }
  }

  private val nonDecimalNameToType = {

    val s = Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)

    (s.map(t => t.typeName -> t) ++ s.map(t => t.simpleString -> t)).toMap
  }
}