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

package hydra.spark.app.parser

import com.typesafe.config._
import configs.syntax._
import hydra.spark.api._
import hydra.spark.internal.Logging

import scala.util.Try

/*
{
  topics:"" <<OR>>
  topicPattern:""
  startingOffsets:""
  sink:
}
 */
object TypesafeReplicationParser extends DSLParser with Logging {


  def parse(dsl: String): Try[ReplicationDetails] = {
    parse(ConfigFactory.parseString(dsl, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)))
  }

  def parse(dsl: Config): Try[ReplicationDetails] = {

    Try(dsl.resolve()).map { cfg =>

      val r = cfg.get[Config]("replicate")
        .valueOrThrow(_ => InvalidDslException("Not a valid replication DSL."))

      val topicList = r.get[String]("topics")
      val topicsPattern = r.get[String]("topicsPattern")
      if (topicList.isSuccess && topicsPattern.isSuccess)
        throw InvalidDslException("Only one of `topics` or `topicsPattern` is allowed.")

      if (topicList.isFailure && topicsPattern.isFailure)
        throw InvalidDslException("One of `topics` or `topicsPattern` is required.")

      val startingOffsets = r.get[String]("startingOffsets").valueOrElse("earliest")

      val sink = "jdbc"

      val topics = topicList.map(t => Left(t.split(",").toList))
        .valueOrElse(Right(topicsPattern.value))
      ReplicationDetails("name", topics, startingOffsets, sink)
    }
  }

  override def supports(dsl: String): Boolean = {
    ConfigFactory.parseString(dsl, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
      .get[Config]("replicate").isSuccess
  }

  override def createJob(dsl: String): Try[HydraSparkJob] = parse(dsl).map(d => null)
}