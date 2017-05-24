package hydra.spark.server.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.log.LoggingAdapter
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.server.marshalling.{GenericResponse, HydraSparkJsonSupport}

import scala.util.{Failure, Success, Try}


/**
  * Created by alexsilva on 4/28/17.
  */
class DslEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraSparkJsonSupport {

  val parser = TypesafeDSLParser()

  override def route: Route = post {
    path("dsl") {
      pathEndOrSingleSlash {
        entity(as[String]) { dsl =>
          tryParse(dsl)
        }
      }
    }
  }

  private def tryParse(dsl: String): Route = {
    Try(parser.parse(dsl)) match {
      case Success(dispatch) =>
        complete(GenericResponse(StatusCodes.OK, "Submitted"))
      case Failure(ex) =>
        complete(GenericResponse(StatusCodes.BadRequest, ex.getMessage))
    }
  }
}
