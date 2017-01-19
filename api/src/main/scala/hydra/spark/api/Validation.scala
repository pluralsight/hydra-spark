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

package hydra.spark.api

/**
 * Created by alexsilva on 6/17/16.
 */
trait Validatable {

  /**
   * Validates the target configuration as a future.
   *
   * @return
   */
  def validate: ValidationResult
}

case class ValidationError(origin: String, messages: Seq[String], args: Any*) {

  lazy val message = s"$origin: ${messages.last}"

}

object ValidationError {

  def apply(origin: String, message: String, args: Any*) = new ValidationError(origin, Seq(message), args: _*)

}

/**
 * A validation result.
 */
sealed trait ValidationResult

/**
 * Validation was a success.
 */
case object Valid extends ValidationResult

/**
 * Validation was a failure.
 *
 * @param errors the resulting errors
 */
case class Invalid(errors: Seq[ValidationError]) extends ValidationResult {

  /**
   * Combines these validation errors with another validation failure.
   *
   * @param other validation failure
   * @return a new merged `Invalid`
   */
  def ++(other: Invalid): Invalid = Invalid(this.errors ++ other.errors)
}

/**
 * This object provides helper methods to construct `Invalid` values.
 */
object Invalid {

  /**
   * Creates an `Invalid` value with a single error.
   *
   * @param error the validation error
   * @return an `Invalid` value
   */
  def apply(error: ValidationError): Invalid = Invalid(Seq(error))

  /**
   * Creates an `Invalid` value with a single error.
   *
   * @param error the validation error message
   * @param args  the validation error message arguments
   * @return an `Invalid` value
   */
  def apply(origin: String, error: String, args: Any*): Invalid = Invalid(Seq(ValidationError(origin, error, args: _*)))
}
