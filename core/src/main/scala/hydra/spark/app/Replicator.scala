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

package hydra.spark.app

import hydra.spark.app.ReplicatorParser.Config

import scala.language.existentials

/**
  * Replicates a Kafka topic into a destination.
  */
object Replicator extends App {
  ReplicatorParser.Parser.parse(args, Config()) match {
    case Some(config) =>

    case None =>
    // arguments are bad, error message will have been displayed
  }
}

object ReplicatorParser {

  case class Config(bootstrapServers: String = "localhost:9092",
                    schemaRegistryUrl: String = "http://localhost:8081",
                    applicationId: String = "",
                    topics: String = "",
                    primaryKey: String = "",
                    dbProfile: String = "")

  val Parser = new scopt.OptionParser[Config]("") {
    head("Hydra Kafka Replicator", "0.6.x")

    opt[String]('b', "bootstrap.servers").required().action((x, c) =>
      c.copy(bootstrapServers = x)).text("Kafka bootstrap servers")

    opt[String]('s', "schema.registry.url").required().valueName("<schema registry>").
      action((x, c) => c.copy(schemaRegistryUrl = x)).
      text("The schema registry url, such as http://localhost:8081")

    opt[String]('a', "application.id").required().valueName("<application id>").
      action((x, c) => c.copy(schemaRegistryUrl = x)).
      text("The Kafka application id")

    opt[String]('t', "topics").required().valueName("<kafka topic(s)>").
      action((x, c) => c.copy(topics = x)).
      text("The Kafka topics (RegEx allowed.)")

    opt[String]("db").required().valueName("<db profile>").
      action((x, c) => c.copy(dbProfile = x)).
      text("The Db profile to use")

    opt[String]("pk").optional().valueName("<primary key>").
      action((x, c) => c.copy(primaryKey = x)).
      text("The table(s) primary key(s).")
  }
}
