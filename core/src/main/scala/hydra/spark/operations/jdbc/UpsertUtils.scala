package hydra.spark.operations.jdbc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Created by alexsilva on 3/11/17.
  */
object UpsertUtils {

  /**
    * "Flattens" a dataframe, using the delimiter character to join paths.
    *
    * @param df
    * @param delimiter
    * @return
    */
  def flatten(df: DataFrame, delimiter: String = "_"): DataFrame = {
    val paths = columnPaths(df.schema)
    val dfColumns = paths.map(path => df.col(path).alias(path.replace(".", delimiter)))
    df.select(dfColumns: _*)
  }

  /**
    * Returns a list of paths to all the columns in this dataframe.
    *
    * For instance, given a dataframe with the following json:
    *
    * {
    * "context": {
    * "ip": "127.0.0.1"
    * },
    * "user": {
    * "handle": "alex",
    * "names": {
    * "first": "alex",
    * "last": "silva"
    * },
    * "id": 123
    * }
    * }
    *
    * The return list of paths will be:
    * (context.ip, user.handle, user.id, user.names.first, user.names.last)
    *
    * Keep in mind that ordering is not guaranteed.
    *
    * @param schema     The original schema
    * @param parentPath Specify the path of the parent element
    */
  def columnPaths(schema: StructType, parentPath: Option[String] = None): Seq[String] = {
    schema.fields.flatMap(field => {
      val colName = parentPath.map(_ + "." + field.name).getOrElse(field.name)
      field.dataType match {
        case st: StructType => columnPaths(st, Some(colName))
        case _ => Array(colName)
      }
    })
  }
}
