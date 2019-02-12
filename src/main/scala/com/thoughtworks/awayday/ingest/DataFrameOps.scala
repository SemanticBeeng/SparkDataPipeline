package com.thoughtworks.awayday.ingest

import cats.kernel.Semigroup
import com.thoughtworks.awayday.ingest.models.ErrorModels.DataError
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._

object DataFrameOps {

  import UDFs._

  implicit class DFOps(wrappedDf: DataFrame) {
    def replaceChars(map: Map[String, String], cols: Seq[String]): DataFrame = {
      val origColOrder = wrappedDf.columns
      val unorderedDf = cols.foldLeft(wrappedDf) { case (df, col) =>
        df.withColumn(col, replaceCharUdf(map)(df(col)))
      }
      unorderedDf.select(origColOrder.map(col): _*)
    }
  }

  implicit def dataFrameSemigroup[A:Encoder]: Semigroup[Dataset[A]] = new Semigroup[Dataset[A]] {
    override def combine(x: Dataset[A], y: Dataset[A]): Dataset[A] = x.union(y)
  }

  implicit val errorEncoder = Encoders.product[DataError]

  val emptyErrorStream = (spark:SparkSession) => {
    implicit val sqlC = spark.sqlContext
    MemoryStream[DataError].toDS()
  }

 /* val emptyErrors = (spark: SparkSession, encoder: Encoder[DataError]) => {
    spark.emptyDataset[DataError]
  }*/
}
