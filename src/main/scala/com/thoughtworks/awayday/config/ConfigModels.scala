package com.thoughtworks.awayday.config

import java.net.URL

import cats.syntax.either._
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig._
// @formatter:off

object ConfigModels {

  case class IngestionConfig(global: GlobalConfigs, datasets: List[DatasetConfig])

  case class GlobalConfigs()

  //Dataset
  sealed trait DatasetConfig {
    def name: String
    def filePattern: String
    def fileFormat: FileFormat
    def dataShape: String
    def storage: List[Storage]
    def pipeline: List[StageConfig]
    def columns: List[DataColumn]

  }

  case class BatchSourceDatasetConfig(name: String,
                                      filePattern: String,
                                      fileFormat: FileFormat,
                                      dataShape: String,
                                      storage: List[Storage],
                                      pipeline: List[StageConfig],
                                      columns: List[DataColumn]
                                     ) extends DatasetConfig

  case class FixedWidthSourceDatasetConfig(name: String,
                                           filePattern: String,
                                           fileFormat: FileFormat,
                                           dataShape: String,
                                           storage: List[Storage],
                                           pipeline: List[StageConfig],
                                           columns: List[DataColumn]
                                          ) extends DatasetConfig


  //File formats
  sealed trait FileFormat

  case class Delimited(delimiter: String) extends FileFormat
  case class FixedWidth(lenConfigs: List[LengthConfig]) extends FileFormat
  case class LengthConfig(name: String, length: String)

  //Storage
  sealed trait Storage

  case class HDFSStorage(user: String, root: String) extends Storage
  case class HiveStorage(user: String, database: String) extends Storage
  case class HBaseStorage(user: String, database: String) extends Storage
  case class RedShiftStorage(user: String, database: String) extends Storage

  //Stage Config
  sealed trait StageConfig

  case class AddRowKeyStageConfig(params: Option[Config]) extends StageConfig
  case class WriteToHBaseStageConfig(params: Option[Config]) extends StageConfig
  case class ReplaceCharDataStageConfig(params: Option[Config]) extends StageConfig
  case class DataTypeValidatorStageConfig(params: Option[Config]) extends StageConfig
  case class DataTypeCastStageConfig(params: Option[Config]) extends StageConfig

  //Columns
  case class DataColumn(name: String, dType: String, format: String="", nullable: Boolean = true, primaryKey: Boolean = false)


  def getIngestionConfig(applicationConfigURI: URL, root: String): Either[Throwable, IngestionConfig] = {
    Either.catchNonFatal {
      val appConfig = ConfigFactory.parseURL(applicationConfigURI).resolve()
      get(appConfig, root)
    }
  }

  def get(config: Config, rootKey: String): IngestionConfig = {

    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    implicit val dataSetSourceTypeConfHint = new FieldCoproductHint[DatasetConfig]("sourceType") {
      override def fieldValue(name: String) = name
    }

    implicit val storageTypeConfHint = new FieldCoproductHint[Storage]("storageType") {
      override def fieldValue(name: String) = name
    }

    implicit val fileFormatTypeConfHint = new FieldCoproductHint[FileFormat]("fType") {
      override def fieldValue(name: String) = name
    }

    implicit val stageTypeConfHint = new FieldCoproductHint[StageConfig]("stageType") {
      override def fieldValue(name: String) = name
    }

    /*implicit val dataStageTypeHint = new FieldCoproductHint[DataStage[DataFrame]]("sType") {
      override def fieldValue(name: String) = name
    }*/

    val retConfigs = loadConfigOrThrow[IngestionConfig](config, rootKey)
    retConfigs
  }

}