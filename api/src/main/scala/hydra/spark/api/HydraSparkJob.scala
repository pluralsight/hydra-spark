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

import scala.language.existentials

/**
  * Created by alexsilva on 6/17/16.
  */
trait HydraSparkJob extends Validatable {

  /**
    * A unique id identifying this dispatch job.
    * It is important it is unique but reproducible because it is used in
    * Spark checkpointing.
    *
    * @return
    */
  def id: String

  /**
    * A user-provided name for this dispatch. (Optional)
    * If none is provided, the id is used.
    *
    * @return
    */
  def name: String = id

  /**
    * The person (or machine?) who wrote this dispatch.
    *
    * @return
    */
  def author: String


  def run(): Unit

  def awaitTermination(): Unit

  def stop(): Unit
}