package com.thoughtworks.awayday.config

import com.thoughtworks.awayday.config.ConfigModels.AddRowKeyStageConfig
import com.thoughtworks.awayday.ingest.base.PipelineTestBase
import org.scalatest.{FlatSpec, Matchers}

class ConfigModelParserTest extends FlatSpec with Matchers with PipelineTestBase {

  "ConfigModelParserTest" should "parse all configs from the config correctly" in {

    val ingestionConfig = getIngestionConfig("/datasets_test.conf")

    val dataSets = ingestionConfig.datasets

    val firstDataSet = dataSets.head
    firstDataSet.dataShape shouldBe "Fact"

    val storages = firstDataSet.storage
    storages.length shouldBe 1

    val pipeline = dataSets.head.pipeline
    pipeline.length shouldBe 4

    val config = pipeline.head.asInstanceOf[AddRowKeyStageConfig].params
    config.foreach(_.getString("xx") shouldBe "yy")

    println (firstDataSet)
  }
}
