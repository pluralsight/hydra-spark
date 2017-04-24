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

package hydra.spark.testutils

/**
  * Created by alexsilva on 1/9/17.
  */

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  @transient private var _sc: SparkContext = _
  @transient private var _session: SparkSession = _

  def sc: SparkContext = _sc

  def ss: SparkSession = _session

  var conf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("hydra-test")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)

  override def beforeAll() {
    super.beforeAll()
    _session = SparkSession.builder().config(conf).appName("hydra-test").getOrCreate()
    _sc = _session.sparkContext
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    DebugFilesystem.assertNoOpenStreams()
  }

  object TestImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.ss.sqlContext
  }

}
