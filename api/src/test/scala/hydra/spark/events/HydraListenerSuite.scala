/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.events

import java.util.concurrent.Semaphore

import com.typesafe.config.ConfigFactory
import hydra.spark.api.{LocalSparkContext, ResetSystemProperties}
import org.apache.spark._
import org.scalatest.{FlatSpecLike, Matchers}

class HydraListenerSuite extends Matchers with FlatSpecLike with LocalSparkContext with ResetSystemProperties {

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000

  val jobCompletionTime = 1421191296660L


  "The AsyncListenerBus" should "be created and shutdown" in {
    val conf = ConfigFactory.empty()
    val counter = new BasicJobCounter
    val bus = new AsyncListenerBus(conf)
    bus.addListener(counter)

    (1 to 5).foreach { _ => bus.post(HydraTransformationEnd(0, jobCompletionTime, JobSucceeded, null)) }

    assert(counter.count === 0)

    // Starting listener bus should flush all buffered events
    bus.start(sc)
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(counter.count === 5)

    // After listener bus has stopped, posting events should not increment counter
    bus.stop()
    (1 to 5).foreach { _ => bus.post(HydraTransformationEnd(0, jobCompletionTime, JobSucceeded, null)) }
    assert(counter.count === 5)


    // Listener bus must not be started twice
    intercept[IllegalStateException] {
      val bus = new AsyncListenerBus(conf)
      bus.start(sc)
      bus.start(sc)
    }

    // ... or stopped before starting
    intercept[IllegalStateException] {
      val bus = new AsyncListenerBus(conf)
      bus.stop()
    }
  }

  "bus.stop()" should "wait for the event queue to completely drain" in {
    @volatile var drained = false

    // When Listener has started
    val listenerStarted = new Semaphore(0)

    // Tells the listener to stop blocking
    val listenerWait = new Semaphore(0)

    // When stopper has started
    val stopperStarted = new Semaphore(0)

    // When stopper has returned
    val stopperReturned = new Semaphore(0)

    class BlockingListener extends BaseHydraListener {
      override def onTransformationEnd(jobEnd: HydraTransformationEnd): Unit = {
        listenerStarted.release()
        listenerWait.acquire()
        drained = true
      }
    }
    val conf = ConfigFactory.empty()
    val bus = new AsyncListenerBus(conf)
    val blockingListener = new BlockingListener

    bus.addListener(blockingListener)
    bus.start(sc)
    bus.post(HydraTransformationEnd(0, jobCompletionTime, JobSucceeded, null))

    listenerStarted.acquire()
    // Listener should be blocked after start
    assert(!drained)

    new Thread("ListenerBusStopper") {
      override def run() {
        stopperStarted.release()
        // stop() will block until notify() is called below
        bus.stop()
        stopperReturned.release()
      }
    }.start()

    stopperStarted.acquire()
    // Listener should remain blocked after stopper started
    assert(!drained)

    // unblock Listener to let queue drain
    listenerWait.release()
    stopperReturned.acquire()
    assert(drained)
  }


  "HydraListener" should "move on if a listener throws an exception" in {
    val badListener = new BadListener
    val jobCounter1 = new BasicJobCounter
    val jobCounter2 = new BasicJobCounter
    val bus = new AsyncListenerBus(ConfigFactory.empty)

    // Propagate events to bad listener first
    bus.addListener(badListener)
    bus.addListener(jobCounter1)
    bus.addListener(jobCounter2)
    bus.start(sc)

    // Post events to all listeners, and wait until the queue is drained
    (1 to 5).foreach { _ => bus.post(HydraTransformationEnd(0, jobCompletionTime, JobSucceeded, null)) }
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // The exception should be caught, and the event should be propagated to other listeners
    assert(bus.listenerThreadIsAlive)
    assert(jobCounter1.count === 5)
    assert(jobCounter2.count === 5)
  }
}

/**
  * A simple listener that throws an exception on job end.
  */
private class BadListener extends BaseHydraListener {
  override def onTransformationEnd(jobEnd: HydraTransformationEnd): Unit = {
    throw new Exception
  }
}


private class BasicJobCounter extends BaseHydraListener {
  var count = 0

  override def onTransformationEnd(jobEnd: HydraTransformationEnd): Unit = count += 1
}

/**
  * A simple listener that tries to stop SparkContext.
  */
private class SparkContextStoppingListener(val sc: SparkContext) extends BaseHydraListener {
  @volatile var sparkExSeen = false

  override def onTransformationEnd(jobEnd: HydraTransformationEnd): Unit = {
    try {
      sc.stop()
    } catch {
      case _: SparkException =>
        sparkExSeen = true
    }
  }
}

private class ListenerThatAcceptsSparkConf(conf: SparkConf) extends BaseHydraListener {
  var count = 0

  override def onTransformationEnd(jobEnd: HydraTransformationEnd): Unit = count += 1
}

