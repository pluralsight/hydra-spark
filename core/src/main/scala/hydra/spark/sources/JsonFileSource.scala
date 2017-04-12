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

package hydra.spark.sources

import com.pluralsight.hydra.hadoop.io.JsonInputFormat
import hydra.spark.util.RDDConversions._
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, DataFrameReader }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by alexsilva on 10/24/16.
 */
case class JsonFileSource(val path: String, val properties: Map[String, Any])
    extends AbstractFileSource[LongWritable, Text, JsonInputFormat] {

  override def createStream(sc: StreamingContext): DStream[Text] = {
    setHadoopConfig(sc.sparkContext)
    val stream = sc.fileStream[LongWritable, Text, JsonInputFormat](path, defaultFilter, false).map {
      case (k, v) => v
    }.filter(_ != null)

    windowDuration.map(stream.window(_, slideDuration.getOrElse(stream.slideDuration))).getOrElse(stream)
  }

  override val fileToDF = (reader: DataFrameReader) => reader.json(path)

  /**
   * Converts an RDD of type S to a dataframe of the same type.
   * Implementations should use the type class RDDConversions
   *
   * @param rdd
   */
  override def toDF(rdd: RDD[Text]): DataFrame = rdd.toDF
}
