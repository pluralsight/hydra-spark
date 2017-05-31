package hydra.spark.server.io

import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime

case class FileInfo(appName: String, btype: BinaryType, path: String, uploadTime: DateTime)