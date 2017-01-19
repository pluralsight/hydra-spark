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

package hydra.spark.util

import hydra.spark.sources.kafka.KafkaMessageAndMetadata
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

/**
 * Created by alexsilva on 8/15/16.
 */
object RDDConversions {

  trait DFLike[T] extends Serializable {
    def toDF(rdd: RDD[T]): DataFrame
  }

  implicit object StringDF extends DFLike[String] {
    override def toDF(rdd: RDD[String]): DataFrame = {
      val spark: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)
      spark.read.json(rdd)
    }
  }

  implicit object TextDF extends DFLike[Text] {
    override def toDF(rdd: RDD[Text]): DataFrame = {
      val spark: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)
      spark.read.json(rdd.map(_.toString))
    }
  }

  implicit object KafkaDF extends DFLike[KafkaMessageAndMetadata[_, _]] {
    type RKMMD = RDD[KafkaMessageAndMetadata[_, _]]

    override def toDF(rdd: RKMMD): DataFrame = {
      val spark: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)
      spark.read.json(rdd.asInstanceOf[RKMMD].map(_.value.toString)).toDF()
    }
  }

  implicit object RowDF extends DFLike[Row] {
    override def toDF(rdd: RDD[Row]): DataFrame = {
      val spark: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)
      val schema = rdd.asInstanceOf[RDD[Row]].first().schema
      spark.createDataFrame(rdd.asInstanceOf[RDD[Row]], schema)
    }
  }

  implicit class RDDConversionsPimp[T](rdd: RDD[T]) {
    def toDF(implicit cl: DFLike[T]) = cl.toDF(rdd)
  }

}