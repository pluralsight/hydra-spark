package hydra.spark.sources

import com.google.common.cache.CacheBuilder

object JdbcRecordWriters {

  private val writers = CacheBuilder.newBuilder().maximumSize(1000).build()




}
