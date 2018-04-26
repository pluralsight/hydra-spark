package hydra.spark.sources

import hydra.spark.replicate.KafkaRecord
import org.apache.spark.sql.ForeachWriter

class JdbcUpsert[K] extends ForeachWriter[KafkaRecord[K]] {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: KafkaRecord[K]): Unit = {

  }

  override def close(errorOrNull: Throwable): Unit = ???
}
