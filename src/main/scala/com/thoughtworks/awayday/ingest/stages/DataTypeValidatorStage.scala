package com.thoughtworks.awayday.ingest.stages

import cats.data.Writer
import com.thoughtworks.awayday.config.ConfigModels.DataColumn
import com.thoughtworks.awayday.ingest.UDFs._
import com.thoughtworks.awayday.ingest.models.ErrorModels.{DataError, DataSetWithErrors}
import com.thoughtworks.awayday.ingest.stages.StageConstants._
import com.thoughtworks.awayday.ingest.stages.base.DataStage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataTypeValidatorStage(dataCols: List[DataColumn])(implicit val spark: SparkSession) extends DataStage[DataFrame] {

  override val stage = getClass.getSimpleName

  def apply(dataRecords: DataFrame): DataSetWithErrors[DataFrame] = validateTypes(dataRecords)

  def validateTypes(data: DataFrame): DataSetWithErrors[DataFrame] = {
    val withErrorsDF = data.withColumn(RowLevelErrorListCol, validateRowUDF(dataCols, stage)(struct(data.columns.map(data(_)): _*)))

    import spark.implicits._

    val errorRecords =
      withErrorsDF
        .select(RowLevelErrorListCol)
        .select(explode(col(RowLevelErrorListCol)))
        .select("col.*")
        .map(row => DataError(row))

    Writer(errorRecords, withErrorsDF.drop(RowLevelErrorListCol))
  }
}