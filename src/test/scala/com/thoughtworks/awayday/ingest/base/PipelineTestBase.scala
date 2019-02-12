package com.thoughtworks.awayday.ingest.base

import com.thoughtworks.awayday.PipelineUtils
import com.thoughtworks.awayday.config.ConfigModels
import com.thoughtworks.awayday.config.ConfigModels.{DatasetConfig, IngestionConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait PipelineTestBase extends BeforeAndAfterAll {
  self: Suite =>

  def getIngestionConfig(configPath: String): IngestionConfig = {
    val configUri = getClass.getResource(configPath)
    ConfigModels.getIngestionConfig(configUri, "ingestionConfig").right.get
  }

  def getDataSetConfig(configPath: String, fileName: String): DatasetConfig = {
    PipelineUtils.findDatasetFromFileName(fileName, getIngestionConfig(configPath)).right.get
  }
}
