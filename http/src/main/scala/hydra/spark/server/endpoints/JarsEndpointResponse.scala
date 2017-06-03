package hydra.spark.server.endpoints

import hydra.spark.server.io.FileInfo

case class FileEndpointResponse(status: Int, message: Option[String], fileInfo: Option[FileInfo])