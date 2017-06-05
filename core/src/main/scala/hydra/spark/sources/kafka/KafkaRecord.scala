package hydra.spark.sources.kafka

/**
  * Created by alexsilva on 5/16/17.
  */
case class KafkaRecord[K, V](key: K, value: V)
