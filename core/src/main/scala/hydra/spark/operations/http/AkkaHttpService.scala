package hydra.spark.operations.http

import hydra.spark.internal.Logging

/**
  * Created by alexsilva on 1/19/17.
  */

trait AkkaHttpService extends Logging {

//  implicit def system: ActorSystem
//
//  implicit def materializer: ActorMaterializer
//
//  implicit def ec: ExecutionContext
//
//  lazy val http = Http()
//
//    def post(df: DataFrame, uri: String): Future[Seq[Done]] = {
//
//    log.info("Posting to" + uri)
//
//    val inspectResponse: (HttpResponse) => Unit = (r) => {
//      if (r.status.isFailure()) throw new RuntimeException(s"Request failed with ${r.status}")
//    }
//
//    val flows = new scala.collection.mutable.ArrayBuffer[Future[Done]]()
//
//    df.toJSON.foreachPartition { partition =>
//      val connectionFlow = Http().outgoingConnection(uri)
//      val flow = Source.fromIterator(() => partition)
//        .map(row => HttpRequest(HttpMethods.POST, row))
//        .via(connectionFlow)
//        .runWith(Sink.foreach(inspectResponse))
//
//      flows += flow
//
//    }
//
//    Future.sequence(flows)
//
//  }
//
//
//    def shutdown(): Unit = {
//    Http().shutdownAllConnectionPools().onComplete { _ =>
//      Await.result(system.whenTerminated, 10.seconds)
//    }
//  }
}
