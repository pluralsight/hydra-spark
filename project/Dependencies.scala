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

  import Versions._

  lazy val postgres = Seq("org.postgresql" % "postgresql" % postgresVersion)

  lazy val dbTesting = Seq("com.typesafe.slick" %% "slick" % "3.2.0" % "test",
    "com.typesafe.slick" %% "slick-testkit" % "3.2.0" % "test",
    "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.2" % "test",
    "com.h2database" % "h2" % "1.4.192" % "test") ++ postgres.map(_ % "test")

  lazy val serviceContainer = Seq("com.github.vonnagy" %% "service-container" % serviceContainerVersion)
    .map(_.excludeAll(
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule(organization = "org.slf4j"),
      ExclusionRule(organization = "com.fasterxml.jackson.core")
    ))

  lazy val akkaHttp = Seq("com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http" % akkaHttpVersion)

  lazy val configExt = Seq("com.github.kxbmap" %% "configs" % kxbmapConfigVersion)

  lazy val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "0.12.0" % "test"

  lazy val kafka = Seq("org.apache.kafka" %% "kafka" % kafkaVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion, embeddedKafka)

  lazy val reflections = Seq("org.reflections" % "reflections" % reflectionsVersion)

  lazy val springCore = Seq("org.springframework" % "spring-core" % springVersion)

  lazy val slf4j = Seq("org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7").map(_.force())

  lazy val spEL = Seq("org.springframework" % "spring-expression" % springVersion)

  val configs = Seq(
    "com.typesafe" % "config" % typeSafeConfigVersion,
    "com.github.kxbmap" %% "configs" % kxbmapConfigVersion)

  lazy val coreTestDeps = Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test")

  lazy val httpTest = Seq("com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test")

  lazy val confluent = Seq(
    "io.confluent" % "kafka-avro-serializer" % confluentVersion,
    "io.confluent" % "kafka-schema-registry" % confluentVersion % "test")
    .map(_.excludeAll(
      ExclusionRule(organization = "com.fasterxml.jackson.core")
    ))

  lazy val avro = Seq("org.apache.avro" % "avro" % avroVersion)

  lazy val guava = Seq("com.google.guava" % "guava" % guavaVersion).map(_.force())

  lazy val lz4 = Seq("org.lz4" % "lz4-java" % "1.4.0")

  val logging = Seq(
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion)

  //  lazy val jackson = Seq("com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  //    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  //    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion)

  lazy val sparkCore = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided")

  lazy val spark = sparkCore ++ Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided",
    "com.databricks" %% "spark-avro" % avroSparkVersion,
    sparkStreamingKafka, sparkTags)

  lazy val slick = Seq(
    "com.typesafe.slick" %% "slick" % slickVersion,
    "com.h2database" % "h2" % h2Version,
    "org.postgresql" % "postgresql" % postgresVersion,
    "commons-dbcp" % "commons-dbcp" % commonsDbcpVersion,
    "org.flywaydb" % "flyway-core" % flywayVersion
    //"com.typesafe.slick" %% "slick-hikaricp" % slickVersion
  )

  lazy val sparkStreamingKafka =  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

  lazy val sparkTags = ("org.apache.spark" %% "spark-tags" % sparkVersion) exclude("org.scalatest", "scalatest")

  lazy val scopt = Seq("com.github.scopt" %% "scopt" % scoptVersion)

  lazy val sprayJson = Seq("io.spray" %% "spray-json" % sprayVersion)

}