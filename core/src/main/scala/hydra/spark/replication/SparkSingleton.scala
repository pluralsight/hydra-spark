package hydra.spark.replication

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

class SparkSingleton[T: ClassTag](init: => T) extends AnyRef with Serializable {

  val id = UUID.randomUUID().toString

  @transient private lazy val instance: T = {
    SparkSingleton.pool.synchronized {
      val singletonOption = SparkSingleton.pool.get(id)
      if (singletonOption.isEmpty) {
        SparkSingleton.pool.put(id, init)
      }
    }

    SparkSingleton.pool.get(id).get.asInstanceOf[T]
  }

  def get = instance
}

object SparkSingleton {

  private val pool = new TrieMap[String, Any]()

  def apply[T: ClassTag](constructor: => T): SparkSingleton[T] = new SparkSingleton[T](constructor)

  def poolSize: Int = pool.size

  def poolClear(): Unit = pool.clear()

}