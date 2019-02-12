package com.thoughtworks.awayday

import com.thoughtworks.awayday.config.ConfigModels.{DatasetConfig, IngestionConfig}
import com.thoughtworks.awayday.ingest.stages.StageConstants
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import cats.syntax.either._

object PipelineUtils {
  def findDatasetFromFileName(fileName: String, config: IngestionConfig): Either[String, DatasetConfig] = {
    Either.fromOption(
      config.datasets.find { dataSet =>
        val pattern = dataSet.filePattern.r
        pattern.findFirstIn(fileName).exists(_.nonEmpty)
      }
      , "Configuration for the fileName pattern not found")
  }

  def stringToDataType(dType: String): DataType = CatalystSqlParser.parseDataType(dType)

  def convertColumnsToStructTypeAndAddRowKey(dataset: DatasetConfig): StructType = {
    StructType(
      StructField(StageConstants.RowKey, StringType, nullable = false) ::
        convertColumnsToStructFields(dataset)
    )
  }

  def convertColumnsToStructType(dataset: DatasetConfig): StructType = {
    StructType(
      convertColumnsToStructFields(dataset)
    )
  }

  def convertColumnsToAllStringStructType(dataset: DatasetConfig): StructType = {
    StructType(
      convertColumnsToStructFields(dataset).map(field => field.copy(dataType = StringType))
    )
  }

  private def convertColumnsToStructFields(dataset: DatasetConfig) = {
    dataset.columns.map { dataCol =>
      StructField(dataCol.name.trim, stringToDataType(dataCol.dType.trim), dataCol.nullable)
    }
  }

  /*  val DoubleCols = EvergreenSchema.fields.filter(_.dataType == DoubleType).map(_.name)
    val StringCols = EvergreenSchema.fields.filter(_.dataType == StringType).map(_.name)
    val FeatureCols = DoubleCols.filterNot(_ == "label")
    val LabelCol = "label"*/
}
