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

import org.scalatest.{ BeforeAndAfterAll, Suite }
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.{ PositionedParameters, SQLActionBuilder, SetParameter }

/**
 * Created by alexsilva on 6/12/16.
 */
trait H2Spec extends Suite with BeforeAndAfterAll {
  val dbname = getClass.getSimpleName.toLowerCase
  private val driver = "org.h2.Driver"

  val h2Url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

  private val h2 = Database.forURL("jdbc:h2:mem", driver = driver)
  h2.run {
    basicUpdate(s"DROP DATABASE IF EXISTS $dbname")
    basicUpdate(s"CREATE DATABASE $dbname")
  }

  override def afterAll() {
    h2.run(basicUpdate(s"DROP DATABASE $dbname"))
  }

  val database = Database.forURL(h2Url, driver = driver)

  private def emptyParameter = SetParameter[Unit]((u: Unit, p: PositionedParameters) => {})

  def basicUpdate(statement: String) =
    new SQLActionBuilder(List(statement), emptyParameter).asUpdate
}