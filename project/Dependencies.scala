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

import sbt._

object Dependencies {

  import ExclusionRules._
  import Versions._

  lazy val postgres = Seq("org.postgresql" % "postgresql" % postgresVersion)

  lazy val dbTesting = Seq("com.typesafe.slick" %% "slick" % "3.1.1" % "test",
    "com.typesafe.slick" %% "slick-testkit" % "3.1.1" % "test",
    "ru.yandex.qatools.embed" % "postgresql-embedded" % "1.19" % "test",
    "com.h2database" % "h2" % "1.4.192" % "test") ++ postgres.map(_ % "test")

  lazy val slick = Seq("com.typesafe.slick" %% "slick" % slickVersion,
    "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
    "org.postgresql" % "postgresql" % postgresVersion)

  lazy val typesafeConfig = Seq("com.typesafe" % "config" % typeSafeConfigVersion)

  lazy val configExt = Seq("com.github.kxbmap" %% "configs" % kxbmapConfigVersion)

  lazy val kafka08 = Seq("org.apache.kafka" %% "kafka" % "0.8.2.2", kafkaUnit)

  lazy val kafkaUnit = "info.batey.kafka" % "kafka-unit" % "0.3" % Test excludeAll (
    ExclusionRule(organization = "org.apache.kafka"))

  lazy val reflections = Seq("org.reflections" % "reflections" % reflectionsVersion)

  lazy val springCore = Seq("org.springframework" % "spring-core" % springVersion)

  lazy val slf4j = Seq("org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7").map(_.force())

  lazy val spEL = Seq("org.springframework" % "spring-expression" % springVersion)

  lazy val coreTestDeps = Seq(
    "org.scalactic" %% "scalactic" % scalaticVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % "test")

  lazy val confluent = Seq("io.confluent" % "kafka-avro-serializer" % confluentVersion excludeAll (excludeJackson: _*))

  lazy val avro = Seq("org.apache.avro" % "avro" % avroVersion)

  lazy val guava = Seq("com.google.guava" % "guava" % guavaVersion).map(_.force())

  lazy val log4J = Seq("org.apache.logging.log4j" % "log4j-api" % "2.7",
    "org.apache.logging.log4j" % "log4j-core" % "2.7")

  lazy val jackson = Seq("com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion)

  lazy val sparkCore = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll (sparkExcludes: _*),
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided" excludeAll (sparkExcludes: _*))

  lazy val spark = sparkCore ++ Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" excludeAll (excludeJackson: _*),
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided" excludeAll (excludeJackson: _*),
    "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided" excludeAll (excludeJackson: _*),
    "com.databricks" %% "spark-csv" % "1.4.0" excludeAll (excludeJackson: _*),
    "com.databricks" %% "spark-avro" % avroSparkVersion,
    sparkStreamingKafka)

  lazy val extraSparkDeps = sparkCassandraConnector ++ sparkElasticSearch

  lazy val sparkElasticSearch = Seq(
    "org.elasticsearch" %% "elasticsearch-spark" % sparkElasticVersion,
    "org.elasticsearch.test" % "framework" % elasticSearchVersion % "test",
    "org.apache.lucene" % "lucene-test-framework" % luceneVersion % "test",
    "org.elasticsearch" % "elasticsearch" % elasticSearchVersion % "provided",
    "com.github.spullara.mustache.java" % "compiler" % "0.8.13" % "provided"
  )

  lazy val sparkCassandraConnector = Seq(
    "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion,
    "org.cassandraunit" % "cassandra-unit" % "2.2.2.1" % "test",
    "net.jpountz.lz4" % "lz4" % "1.3.0" % "test",
    "org.hectorclient" % "hector-core" % "2.0-0" % "test").map(_.force())

  lazy val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion excludeAll (excludeJackson: _*)

  lazy val scopt = Seq("com.github.scopt" %% "scopt" % scoptVersion)

  lazy val sprayJson = Seq( "io.spray" %%  "spray-json" % sprayVersion)

  lazy val akkaHttp = Seq("com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test")

}