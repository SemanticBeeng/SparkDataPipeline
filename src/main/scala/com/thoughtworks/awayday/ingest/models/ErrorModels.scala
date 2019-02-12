package com.thoughtworks.awayday.ingest.models

import cats.data.Writer
import org.apache.spark.sql.{Dataset, Row}

object ErrorModels {

  case class DataError(rowKey: String, stage: String, fieldName: String, fieldValue: String, error: String, severity: String, addlInfo: String = "")

  object DataError{
    def apply(row: Row): DataError = new DataError(
      row.getAs[String]("rowKey"),
      row.getAs[String]("stage"),
      row.getAs[String]("fieldName"),
      row.getAs[String]("fieldValue"),
      row.getAs[String]("error"),
      row.getAs[String]("severity"),
      row.getAs[String]("addlInfo")
    )
  }

  type DataSetWithErrors[A] = Writer[Dataset[DataError], A]


}
