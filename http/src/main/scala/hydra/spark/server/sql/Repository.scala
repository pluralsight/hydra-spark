package hydra.spark.server.sql

import slick.ast.BaseTypedType
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import slick.lifted.AbstractTable

/**
  * Created by alexsilva on 2/9/17.
  */

abstract class Repository[T <: AbstractTable[_], I: BaseTypedType](val profile: JdbcProfile, val db: Database) {

  import profile.api._
  import slick.lifted.TableQuery

  type Id = I

  def table: TableQuery[T]

  def getId(row: T): Rep[Id]

  def filterById(id: Id) = table filter (getId(_) === id)

  def findById(id: Id) = db run filterById(id).result.headOption

  def insert(model: T#TableElementType) = db run (table += model)

  def insertOrUpdate(model: T#TableElementType) = db run (table insertOrUpdate (model))

  def deleteById(id: Id) = {
    val deleteAction = buildDeleteAction(id)
    db run deleteAction.delete
  }


  private def buildDeleteAction(id: Id) = {
    profile.createDeleteActionExtensionMethods(
      profile.deleteCompiler.run(filterById(id).toNode).tree, ()
    )
  }
}

