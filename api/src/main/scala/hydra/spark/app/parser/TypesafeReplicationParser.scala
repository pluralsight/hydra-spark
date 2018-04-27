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

import java.util.UUID

import com.typesafe.config._
import configs.syntax._
import hydra.spark.api._
import hydra.spark.internal.Logging
import hydra.spark.configs._
import scala.util.Try

/*
{
    "replicate": {
        "topics": ["my.Topic", "my.OtherTopic"],
        "topicPattern": "my.*",
        "startingOffsets": "earliest",
        "primaryKeys": "jwt",
        "connectionUri": "JDBC-URL"
    }
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

      require(!r.hasPath("primaryKey"), "primaryKey is not a valid key. Did you mean primaryKeys?")

      val topicList = r.get[List[String]]("topics")
        .orElse(r.get[String]("topics").map(_.split(",").map(_.trim).toList))

      val topicsPattern = r.get[String]("topicPattern")
      if (topicList.isSuccess && topicsPattern.isSuccess)
        throw InvalidDslException("Only one of `topics` or `topicPattern` is allowed.")

      if (topicList.isFailure && topicsPattern.isFailure)
        throw InvalidDslException("One of `topics` or `topicsPattern` is required.")

      val startingOffsets = r.get[String]("startingOffsets").valueOrElse("earliest")
      val connectionInfo = r.get[Config]("connection").valueOrElse(ConfigFactory.empty)

      val topics = topicList.map(t => Left(t)).valueOrElse(Right(topicsPattern.value))

      val name = r.get[String]("name")
        .valueOrElse(UUID.nameUUIDFromBytes(deriveName(topics).getBytes()).toString)

      val pks = r.get[Config]("primaryKeys").map(_.to[Map[String, String]]).valueOrElse(Map.empty)

      ReplicationDetails(name, topics, startingOffsets, pks, connectionInfo)
    }
  }

  private def deriveName(topics: Either[List[String], String]): String = {
    topics match {
      case Right(s) => s
      case Left(topics) => topics.map(_.trim).mkString("")

    }
  }

  override def supports(dsl: String): Boolean = {
    ConfigFactory.parseString(dsl, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
      .get[Config]("replicate").isSuccess
  }

  override def createJob(dsl: String): Try[HydraSparkJob] =
    parse(dsl).map(new KafkaReplicationJob(_))
}