package hydra.spark.util

import java.util.concurrent.CopyOnWriteArrayList

import hydra.spark.internal.Logging

import scala.reflect.ClassTag
import scala.util.control.NonFatal

private[hydra] trait ListenerBus[L <: AnyRef, E] extends Logging {

  private[hydra] val listeners = new CopyOnWriteArrayList[L]

  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  final def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          log.error(s"Listener ${ReflectionUtils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  protected def doPostEvent(listener: L, event: E): Unit

  private[hydra] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    import scala.collection.JavaConverters._
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
