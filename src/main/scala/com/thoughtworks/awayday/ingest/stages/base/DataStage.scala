package com.thoughtworks.awayday.ingest.stages.base

import com.thoughtworks.awayday.ingest.models.ErrorModels.DataSetWithErrors
import org.apache.spark.sql.Dataset

trait DataStage[T <: Dataset[_]] extends Serializable {
  def apply(data: T): DataSetWithErrors[T]
  def stage: String
}
