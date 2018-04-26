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

import Dependencies._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.PathList

val javaVersion = sys.env.getOrElse("JAVA_VERSION", "1.8")

lazy val buildTag = scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")

val jacksonVersion = "2.6.7"

lazy val commonSettings = Seq(
  organization := "pluralsight",
  organizationHomepage := Some(url("https://github.com/pluralsight")),
  version := Versions.hydraSparkVersion + buildTag,
  publishArtifact := true,
  parallelExecution in ThisBuild := false,
  concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
  scalaVersion := "2.11.8",
  excludeDependencies += "org.slf4j" % "slf4j-log4j12",
  excludeDependencies += "log4j" % "log4j",
  excludeDependencies += "log4j" % "apache-log4j-extras",
  excludeDependencies += "net.jpountz.lz4" % "lz4",
  dependencyOverrides += "com.fasterxml.jackson.core" %% "jackson-core" % jacksonVersion,
  dependencyOverrides += "com.fasterxml.jackson.core" %% "jackson-annotations" % jacksonVersion,
  dependencyOverrides += "com.fasterxml.jackson.core" %% "jackson-databind" % jacksonVersion,
  dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala" % jacksonVersion,
  dependencyOverrides += "org.scalatest" %% "scalatest" % "3.0.1",
  dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  publishArtifact in Test := false,
  publishMavenStyle := true,
  scalacOptions := Seq("-deprecation", "-feature", "-language:implicitConversions", "-language:postfixOps"),
  libraryDependencies ~= {
    _.map(_.exclude("org.slf4j", "slf4j-jdk14").exclude("org.slf4j", "slf4j-log4j12"))
  },
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("org.apache", "spark", xs@_*) => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case "webapps" => MergeStrategy.discard
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
  ,
  resolvers += Resolver.mavenLocal,
  resolvers += "Confluent" at "http://packages.confluent.io/maven/",
  resolvers += "The New Motion" at "http://nexus.thenewmotion.com/content/groups/public/"
)

buildOptions in docker := BuildOptions(
  cache = false,
  removeIntermediateContainers = BuildOptions.Remove.Always,
  pullBaseImage = BuildOptions.Pull.Always
)

lazy val `api` = (project in file("api"))
  .settings(commonSettings,
    name := "hydra-spark-api",
    libraryDependencies ++= Seq(logging, Dependencies.configs, spark, coreTestDeps, reflections,
      sprayJson).flatten
  )

lazy val `core` = (project in file("core"))
  .dependsOn(api)
  .enablePlugins(sbtdocker.DockerPlugin)
  .settings(commonSettings ++ dockerSettings,
    name := "hydra-spark-core",
    libraryDependencies ++= Seq(logging, spark, guava, postgres, kafka, confluent, slf4j, sprayJson,
      springCore, spEL, scopt, coreTestDeps, dbTesting, hydra).flatten
  )

lazy val publishSettings = Seq(
  licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
  // bintrayOrganization := Some("hydra-spark")
)

val dockerSettings = Seq(
  dockerfile in docker := {
    val artifact: File = assembly.value
    val artifactTargetPath = s"/jars/${artifact.name}"

    new Dockerfile {
      from("gettyimages/spark:2.3.0-hadoop-2.8")
      maintainer("Alex Silva <alex-silva@pluralsight.com>")
      add(artifact, artifactTargetPath)
      //Spark uses Avro 1.7.4 version that comes packaged with Hadoop.
      // We have to manually override the
      // jar in the docker container so that our KafkaAvroSource won't choke.
      addRaw(
        new java.net.URL("http://repo1.maven.org/maven2/org/apache/avro/avro/1.7.7/avro-1.7.7.jar"),
        "/usr/spark-2.3.0/jars")
    }
  },
  imageNames in docker := Seq(
    // Sets the latest tag
    ImageName(s"${organization.value}/hydra-spark:latest"),

    // Sets a name with a tag that contains the project version
    ImageName(
      namespace = Some(organization.value),
      repository = "hydra-spark",
      tag = Some(version.value)
    )
  )
)

