package com.thoughtworks.awayday.ingest.stages

import cats.data.Writer
import com.thoughtworks.awayday.config.ConfigModels.DataColumn
import com.thoughtworks.awayday.ingest.DataFrameOps
import com.thoughtworks.awayday.ingest.UDFs.replaceCharUdf
import com.thoughtworks.awayday.ingest.models.ErrorModels.{DataError, DataSetWithErrors}
import com.thoughtworks.awayday.ingest.stages.base.DataStage
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class ReplaceCharDataStage(replaceMap: Map[String, String],
                           dataCols:List[DataColumn],
                           applicableCols: Seq[String])
                          (implicit spark: SparkSession, encoder: Encoder[DataError])
  extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(dataRecords: DataFrame): DataSetWithErrors[DataFrame] = replaceChars(dataRecords)

  def replaceChars(data: DataFrame): DataSetWithErrors[DataFrame] = {
    val origColOrder = dataCols.map(_.name)
    val unorderedDf = applicableCols.foldLeft(data) { case (df, col) =>
      df.withColumn(col, replaceCharUdf(replaceMap)(df(col)))
    }
    val returnDf = unorderedDf.select(origColOrder.map(col): _*)
    Writer(DataFrameOps.emptyErrorStream(spark), returnDf)
  }
}
