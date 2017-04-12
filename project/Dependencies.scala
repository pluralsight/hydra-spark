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
    "ru.yandex.qatools.embed" % "postgresql-embedded" % "1.19" % "test",
    "com.h2database" % "h2" % "1.4.192" % "test") ++ postgres.map(_ % "test")

  lazy val slick = Seq("com.typesafe.slick" %% "slick" % slickVersion,
    "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
    "org.postgresql" % "postgresql" % postgresVersion)

  lazy val akkaHttp = Seq("com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion
    , "com.typesafe.akka" %% "akka-http" % akkaHttpVersion)

  lazy val typesafeConfig = Seq("com.typesafe" % "config" % typeSafeConfigVersion)

  lazy val configExt = Seq("com.github.kxbmap" %% "configs" % kxbmapConfigVersion)

  lazy val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "0.12.0" % "test"

  lazy val kafka = Seq("org.apache.kafka" %% "kafka" % kafkaVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    embeddedKafka)

  lazy val reflections = Seq("org.reflections" % "reflections" % reflectionsVersion)

  lazy val springCore = Seq("org.springframework" % "spring-core" % springVersion)

  lazy val slf4j = Seq("org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7").map(_.force())

  lazy val spEL = Seq("org.springframework" % "spring-expression" % springVersion)

  val scalaConfigs = Seq("com.github.kxbmap" %% "configs" % kxbmapConfigVersion)

  lazy val coreTestDeps = Seq(
  //  "org.scalactic" %% "scalactic" % scalaTestVersion % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test")

  lazy val httpTest = Seq("com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test")

  lazy val confluent = Seq(
    "io.confluent" % "kafka-avro-serializer" % confluentVersion,
    "io.confluent" % "kafka-schema-registry" % confluentVersion % "test")

  lazy val avro = Seq("org.apache.avro" % "avro" % avroVersion)

  lazy val guava = Seq("com.google.guava" % "guava" % guavaVersion).map(_.force())

  val logging = Seq(
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion)

  lazy val jackson = Seq("com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion)

  lazy val sparkCore = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided")

  lazy val spark = sparkCore ++ Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided",
    "com.databricks" %% "spark-avro" % avroSparkVersion,
    sparkStreamingKafka, sparkTags)

  lazy val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

  lazy val sparkTags = ("org.apache.spark" %% "spark-tags" % sparkVersion) exclude("org.scalatest", "scalatest")

  lazy val scopt = Seq("com.github.scopt" %% "scopt" % scoptVersion)

  lazy val sprayJson = Seq("io.spray" %% "spray-json" % sprayVersion)

}