package hydra.spark.server.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.log.LoggingAdapter
import hydra.spark.server.io.FileInfo
import hydra.spark.server.marshalling.{GenericResponse, HydraSparkJsonSupport}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/31/17.
  */
class JarsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraSparkJsonSupport {

  implicit val timeout = Timeout(10.seconds)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val binariesEndpointResponseFormat = jsonFormat3(JarsEndpointResponse)

  lazy val bm = system.actorSelection("/user/service/binary_file_manager")

  override val route =
    pathPrefix("jars") {
      handleExceptions(excptHandler) {
        get {
          complete("OK")
        }
      }
    }


  lazy val excptHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        log.warn(s"Request to $uri could not be handled normally")
        complete(BadRequest,
          GenericResponse(400, s"Unable to complete request for ${uri.path.tail} : ${e.getMessage}"))
      }
  }

  case class JarsEndpointResponse(status: Int, message: Option[String], dataFileInfo: Option[FileInfo])

}

