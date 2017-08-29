package hydra.spark.events

import hydra.spark.api.HydraContext

trait HydraListener {
  def onTransformationStart(transStart: HydraTransformationStart): Unit

  def onTransformationEnd(transEnd: HydraTransformationEnd): Unit

  def onOtherEvent(event: HydraListenerEvent): Unit
}

trait BaseHydraListener extends HydraListener {
  override def onTransformationStart(transStart: HydraTransformationStart): Unit = {}

  override def onTransformationEnd(transEnd: HydraTransformationEnd): Unit = {}

  override def onOtherEvent(event: HydraListenerEvent): Unit = {}
}

trait HydraListenerEvent {
  /* Whether output this event to the event log */
  protected[hydra] def logEvent: Boolean = true
}

case class HydraTransformationEnd(jobId: Int, time: Long, jobResult: JobResult, hydraContext: HydraContext)
  extends HydraListenerEvent


case class HydraTransformationStart(jobId: Int, time: Long, hydraContext: HydraContext) extends HydraListenerEvent