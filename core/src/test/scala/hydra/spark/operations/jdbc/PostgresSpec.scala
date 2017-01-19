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

package hydra.spark.dsl.jdbc

import hydra.spark.internal.Logging
import org.scalatest.{ BeforeAndAfterAll, Suite }
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig
import ru.yandex.qatools.embed.postgresql.{ PostgresExecutable, PostgresProcess, PostgresStarter }
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.{ PositionedParameters, SQLActionBuilder, SetParameter }

/**
 * Created by alexsilva on 6/12/16.
 */
trait PostgresSpec extends Suite with BeforeAndAfterAll with Logging {
  val dbname = "postgres"
  private val driver = "org.postgresql.Driver"

  val pgConfig = PostgresConfig.defaultWithDbName("hydra")

  val url = s"jdbc:postgresql://${pgConfig.net().host()}:${pgConfig.net().port()}/" +
    s"${pgConfig.storage().dbName()}"

  lazy val pg = Database.forURL(url, driver = driver)

  var exec: Option[PostgresExecutable] = None

  val runtime: PostgresStarter[PostgresExecutable, PostgresProcess] = PostgresStarter.getDefaultInstance()

  override def beforeAll(): Unit = {
    super.beforeAll()
    exec = Some(runtime.prepare(pgConfig))
    exec.foreach(_.start())
  }

  override def afterAll() {
    super.afterAll()
    log.info("STOPPING POSTGRES")
    exec.foreach(_.stop())

  }

  lazy val database = Database.forURL(url, driver = driver)

  def emptyParameter = SetParameter[Unit]((u: Unit, p: PositionedParameters) => {})

  def basicUpdate(statement: String) =
    new SQLActionBuilder(List(statement), emptyParameter).asUpdate
}