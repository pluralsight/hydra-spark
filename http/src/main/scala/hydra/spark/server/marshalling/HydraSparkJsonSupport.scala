package hydra.spark.server.marshalling

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

/**
  * Created by alexsilva on 4/28/17.
  */
trait HydraSparkJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val genericResponseFormat = jsonFormat3(GenericResponse)
}
