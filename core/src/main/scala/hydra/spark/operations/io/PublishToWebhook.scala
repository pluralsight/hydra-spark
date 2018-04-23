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
import hydra.spark.internal.Logging
import hydra.spark.operations.http.AkkaHttpService
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration._

/**
  *
  * Posts each row in the dataframe to the url
  *
  * This is just a stub - Need to add header, timeouts, etc.
  *
  * Created by alexsilva on 6/21/16.
  */
case class PublishToWebhook(url: String, timeout: FiniteDuration = 1.minute) extends DFOperation
  with Logging
  with AkkaHttpService {

  override val id: String = s"publish-to-web-hook-$url"

  override def transform(df: DataFrame): DataFrame = {
    //TODO: Look into Akka HTTP with Spark 1.6
    //Await.result(post(df, url), timeout)
    df
  }

  override def validate: ValidationResult = checkRequiredParams(Seq(("url", url)))
}

object PublishToWebhook {

  import configs.syntax._

  def apply(cfg: Config): PublishToWebhook = {
    val url = cfg.get[String]("url").valueOrThrow(_ => new InvalidDslException("Url is a required property"))
    val timeout = cfg.get[FiniteDuration]("timeout").valueOrElse(1.minute)

    PublishToWebhook(url, timeout)
  }
}

