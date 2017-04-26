package hydra.spark.operations.common

import hydra.spark.operations.jdbc.UpsertUtils
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by alexsilva on 3/11/17.
  */
private[operations] case class TableMapping(idColumn: Option[ColumnMapping], columns: Seq[ColumnMapping]) {

  def targetDF(df: DataFrame): DataFrame = {
    columns.headOption.map { _ =>
      //only executes if there are target columns
      val cols = columns.map(getColumn(df, _))
      //add the id if present
      val colsWithId = idColumn.map(c => getColumn(df, c) +: cols).getOrElse(cols)
      df.select(colsWithId: _*)
    }.getOrElse {
      //otherwise we use all the columns in the dataframe.
      val allCols: DataFrame = UpsertUtils.flatten(df)
      //rename the id col if needed
      idColumn.map(id => allCols.withColumnRenamed(id.source.replace(".", "_"), id.target)).getOrElse(allCols)
    }
  }

  private def getColumn(df: DataFrame, mapping: ColumnMapping): Column = {
    df(mapping.source).alias(mapping.target).cast(mapping.`type`)
  }
}