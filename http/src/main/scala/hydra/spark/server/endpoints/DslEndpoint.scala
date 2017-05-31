package hydra.spark.server.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.log.LoggingAdapter
import hydra.spark.api.DispatchDetails
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.server.marshalling._
import hydra.spark.submit.HydraSparkSubmit
import org.apache.spark.launcher.SparkAppHandle

import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}


/**
  * Created by alexsilva on 4/28/17.
  */
class DslEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraSparkJsonSupport {

  val parser = TypesafeDSLParser()

  val dsls = new mutable.HashMap[String, SparkAppHandle]()

  override def route: Route =
    post {
      path("streams") {
        pathEndOrSingleSlash {
          entity(as[String]) { dsl =>
            tryParse(dsl)
          }
        }
      }
    } ~ get {
      path("streams" / Segment) { id =>
        streamInfo(id)
      }
    }

  private def streamInfo(id: String): Route = {
    val handle = dsls.get(id)
    handle match {
      case Some(h) => complete(JobStatusResponse(id, Option(h.getAppId), h.getState.name()))
      case None => complete(GenericResponse(StatusCodes.NotFound.intValue, s"No job $id found."))
    }
  }

  private def tryParse(dsl: String): Route = {
    Try(parser.parse(dsl)) match {
      case Success(dispatch) =>
        val handle = trySubmit(dispatch)
        val completion: ToResponseMarshallable = handle match {
          case Success(h) => h
          case Failure(ex) => GenericResponse(StatusCodes.BadRequest.intValue, ex.getMessage)
        }
        complete(completion)
      case Failure(ex) =>
        complete(GenericResponse(StatusCodes.BadRequest.intValue, ex.getMessage))
    }
  }

  private def trySubmit(dispatch: DispatchDetails[_]): Try[JobSubmittedResponse] = {
    val sparkSubmit = HydraSparkSubmit(dispatch)
    val handle = sparkSubmit.submit()
    handle.map { h =>
      val id = Random.alphanumeric.take(8).mkString
      dsls.put(id, h)
      JobSubmittedResponse(id, sparkSubmit.logFile.getAbsolutePath)
    }
  }
}
