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
import Versions._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.PathList
import sbtbuildinfo.BuildInfoKeys
import sbtdocker.DockerPlugin.autoImport._

object HydraSparkBuild extends Build with BuildInfoKeys {

  val javaVersion = sys.env.getOrElse("JAVA_VERSION", "1.8")


  lazy val `core` = (project in file("core"))
    .settings(commonSettings: _*)
    .dependsOn(api)
    .settings(
      name := "hydra-spark-core",
      libraryDependencies ++= Seq(log4J, spark, guava, postgres, kafka08, confluent, jackson, slf4j, sprayJson,
        reflections, springCore, spEL, scopt, coreTestDeps, dbTesting).flatten
    )

  lazy val `api` = (project in file("api"))
    .settings(commonSettings: _*).
    settings(
      name := "hydra-spark-api",
      libraryDependencies ++= Seq(log4J, spark, guava, postgres, kafka08, confluent, jackson, typesafeConfig,
        reflections, springCore, spEL, scopt, coreTestDeps, dbTesting).flatten
    )

  lazy val extras = (project in file("extras"))
    .settings(commonSettings: _*)
    .dependsOn(core)
    .settings(
      name := "hydra-spark-extras",
      libraryDependencies ++= Seq(log4J, spark, guava, extraSparkDeps, coreTestDeps).flatten
    )

  lazy val buildTag = scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")

  lazy val commonSettings = Seq(
    organization := "pluralsight",
    organizationHomepage := Some(url("https://github.com/pluralsight")),
    version := Versions.hydraSparkVersion + buildTag,
    publishArtifact := true,
    parallelExecution in ThisBuild := false,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    excludeDependencies += "org.slf4j" % "slf4j-log4j12",
    excludeDependencies += "log4j" % "log4j",
    dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    publishArtifact in Test := false,
    publishMavenStyle := true,
    isSnapshot := true,
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
    resolvers += "The New Motion" at "http://nexus.thenewmotion.com/content/groups/public/",
    resolvers += "Con Jars" at "http://conjars.org/repo/",
    resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
  )

  lazy val publishSettings = Seq(
    licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
    // bintrayOrganization := Some("hydra-spark")
  )

  lazy val dockerSettings = Seq(
    // Make the docker task depend on the assembly task, which generates a fat JAR file
    docker <<= (docker dependsOn (assembly in extras)),
    dockerfile in docker := {
      val artifact = (assemblyOutputPath in assembly in extras).value
      val artifactTargetPath = s"/app/${artifact.name}"

      val sparkBuild = s"spark-$sparkVersion"
      val sparkBuildCmd = scalaBinaryVersion.value match {
        case "2.10" =>
          "./make-distribution.sh -Phadoop-2.4 -Phive"
        case "2.11" =>
          """
            |./dev/change-scala-version.sh 2.11 && \
            |./make-distribution.sh -Dscala-2.11 -Phadoop-2.4 -Phive
          """.stripMargin.trim
        case other => throw new RuntimeException(s"Scala version $other is not supported!")
      }

      new sbtdocker.mutable.Dockerfile {
        from(s"java:$javaVersion")
        runRaw(
          """echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list && \
                apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
                apt-get -y update && \
                apt-get -y install mesos=${MESOS_VERSION} && \
                apt-get clean
             """)
        copy(artifact, artifactTargetPath)
        copy(baseDirectory(_ / "bin" / "server_start.sh").value, file("app/server_start.sh"))
        copy(baseDirectory(_ / "bin" / "server_stop.sh").value, file("app/server_stop.sh"))
        copy(baseDirectory(_ / "bin" / "manager_start.sh").value, file("app/manager_start.sh"))
        copy(baseDirectory(_ / "bin" / "setenv.sh").value, file("app/setenv.sh"))
        copy(baseDirectory(_ / "config" / "log4j-stdout.properties").value, file("app/log4j-server.properties"))
        copy(baseDirectory(_ / "config" / "docker.conf").value, file("app/docker.conf"))
        copy(baseDirectory(_ / "config" / "docker.sh").value, file("app/settings.sh"))
        // Including envs in Dockerfile makes it easy to override from docker command
        env("JOBSERVER_MEMORY", "1G")
        env("SPARK_HOME", "/spark")
        // Use a volume to persist database between container invocations
        run("mkdir", "-p", "/database")
        runRaw(
          s"""
             |wget http://d3kbcqa49mib13.cloudfront.net/$sparkBuild.tgz && \\
             |tar -xvf $sparkBuild.tgz && \\
             |cd $sparkBuild && \\
             |$sparkBuildCmd && \\
             |cd .. && \\
             |mv $sparkBuild/dist /spark && \\
             |rm $sparkBuild.tgz && \\
             |rm -r $sparkBuild
        """.stripMargin.trim
        )
        volume("/database")
        entryPoint("app/server_start.sh")
      }
    },
    imageNames in docker := Seq(
      sbtdocker.ImageName(namespace = Some("pluralsight"),
        repository = "hydra-spark",
        tag = Some(s"${version.value}.hydra-spark-$sparkVersion.scala-${scalaBinaryVersion.value}"))
    )
  )

}
