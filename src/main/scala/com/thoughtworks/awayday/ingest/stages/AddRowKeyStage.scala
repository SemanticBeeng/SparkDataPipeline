package com.thoughtworks.awayday.ingest.stages

import cats.data.Writer
import com.thoughtworks.awayday.ingest.DataFrameOps
import com.thoughtworks.awayday.ingest.UDFs.generateUUID
import com.thoughtworks.awayday.ingest.models.ErrorModels.{DataError, DataSetWithErrors}
import com.thoughtworks.awayday.ingest.stages.StageConstants.RowKey
import com.thoughtworks.awayday.ingest.stages.base.DataStage
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


class AddRowKeyStage(schemaWithRowKey: StructType)
                    (implicit spark: SparkSession, encoder: Encoder[DataError])
  extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(dataRecords: DataFrame): DataSetWithErrors[DataFrame] = addRowKeys(dataRecords)

  def addRowKeys(data: DataFrame): DataSetWithErrors[DataFrame] = {
    val colOrder = schemaWithRowKey.fields.map(_.name)
    val withRowKeyDf = data.withColumn(RowKey, lit(generateUUID()))
    val returnDf = withRowKeyDf.select(colOrder.map(col): _*)
    Writer(DataFrameOps.emptyErrorStream(spark), returnDf)
  }
}
