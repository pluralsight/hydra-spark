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

/**
 * Functions for escaping and un-escaping strings. The format is fairly standard and should work
 * for Java, Javascript, and similar languages. Yes, there is a fuzz test for this.
 */
object Quote {

  def quote(s: String) = s.map {
    case '"' => "\\\""
    case '\\' => "\\\\"
    case '/' => "\\/"
    case '\b' => "\\b"
    case '\f' => "\\f"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if ((c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f')) => "\\u%04x".format(c: Int)
    case c => c
  }.mkString("\"", "", "\"")

  def unquote(s: String): String =
    if (s.isEmpty) s else s(0) match {
      case '"' => unquote(s.tail)
      case '\\' => s(1) match {
        case 'b' => '\b' + unquote(s.drop(2))
        case 'f' => '\f' + unquote(s.drop(2))
        case 'n' => '\n' + unquote(s.drop(2))
        case 'r' => '\r' + unquote(s.drop(2))
        case 't' => '\t' + unquote(s.drop(2))
        case '"' => '"' + unquote(s.drop(2))
        case 'u' => Integer.parseInt(s.drop(2).take(4), 16).toChar + unquote(s.drop(6))
        case c => c + unquote(s.drop(2))
      }
      case c => c + unquote(s.tail)
    }

}