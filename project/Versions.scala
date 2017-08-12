

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

object Versions {
  val jdkVersion = scala.util.Properties.isJavaAtLeast("1.8")

  val hydraSparkVersion = "0.6.2"

  lazy val typeSafeConfigVersion = if (jdkVersion) "1.3.0" else "1.2.1"
  lazy val kxbmapConfigVersion = "0.4.2"
  lazy val scalaTestVersion = "3.0.1"
  lazy val kafkaVersion = "0.10.2.0"
  lazy val confluentVersion = "3.2.0"
  lazy val springVersion = "4.2.2.RELEASE"
  lazy val slickVersion = "3.2.0"
  lazy val h2Version = "1.3.176"
  lazy val postgresVersion = "9.4.1209"
  lazy val commonsDbcpVersion = "1.4"
  lazy val flywayVersion = "3.2.1"
  lazy val guavaVersion = "18.0"
  lazy val avroVersion = "1.8.1"
  val log4jVersion = "2.7"
  lazy val reflectionsVersion = "0.9.10"
  lazy val scoptVersion = "3.5.0"
  lazy val sparkVersion = "2.2.0"
  lazy val avroSparkVersion = "3.2.0"
  lazy val jacksonVersion = "2.6.5"
  lazy val akkaHttpVersion = "10.0.5"
  lazy val sprayVersion = "1.3.3"
  lazy val serviceContainerVersion = "2.0.5"
}