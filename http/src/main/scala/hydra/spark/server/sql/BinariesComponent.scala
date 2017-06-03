package hydra.spark.server.sql

import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

trait BinariesComponent {

  this: DatabaseComponent with ProfileComponent =>

  import profile.api._
  import slick.lifted.Tag

  case class Binary(id: Option[Int], appName: String, binaryType: String, uploadTime: DateTime, binary: Array[Byte])

  class BinariesTable(tag: Tag) extends Table[Binary](tag, "BINARIES") {
    def id = column[Int]("BIN_ID", O.PrimaryKey, O.AutoInc)

    def appName = column[String]("APP_NAME")

    def binaryType = column[String]("BINARY_TYPE")

    def uploadTime = column[DateTime]("UPLOAD_TIME")

    def binary = column[Array[Byte]]("BINARY")

    def * = (id.?, appName, binaryType, uploadTime, binary) <> (Binary.tupled, Binary.unapply)
  }

  object BinariesRepository extends Repository[BinariesTable, Int](profile, db) {

    val table = TableQuery[BinariesTable]

    val createQuery = table returning table.map(_.id) into ((item, id) => item.copy(id = Some(id)))

    def create(t: Binary): Future[Binary] = {
      val action = createQuery += t
      db.run(action)
    }

    def getId(table: BinariesTable) = table.id

    def deleteByAppName(appName: String) = {
      def filterByAppName(appName: String) = table filter (_.appName === appName)

      val deleteAction = profile.createDeleteActionExtensionMethods(
        profile.deleteCompiler.run(filterByAppName(appName).toNode).tree, ())
      db run deleteAction.delete
    }

    def fetchBinary(appName: String, bt: BinaryType, uploadTime: DateTime): Future[Binary] = {
      val query = table.filter { b =>
        b.appName === appName && b.uploadTime === uploadTime && b.binaryType === bt.name
      }.result
      db.run(query.head)
    }

    def findByArgs(appName: String, binaryType: BinaryType, uploadTime: DateTime): Future[Binary] = {
      val query = table.filter { bin =>
        bin.appName === appName && bin.uploadTime === uploadTime && bin.binaryType === binaryType.name
      }.result
      db.run(query.head)
    }

    def getApps()(implicit ec: ExecutionContext): Future[Map[String, (BinaryType, DateTime)]] = {
      val query = table.groupBy { r =>
        (r.appName, r.binaryType)
      }.map {
        case ((appName, binaryType), bin) =>
          (appName, binaryType, bin.map(_.uploadTime).max.get)
      }.result
      for (m <- db.run(query)) yield {
        m.map {
          case (appName, binaryType, uploadTime) =>
            (appName, (BinaryType.fromString(binaryType), uploadTime))
        }.toMap
      }
    }

  }

}
