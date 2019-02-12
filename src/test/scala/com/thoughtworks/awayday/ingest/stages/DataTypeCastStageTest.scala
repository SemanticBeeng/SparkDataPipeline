package com.thoughtworks.awayday.ingest.stages

import com.thoughtworks.awayday.config.ConfigModels.DataColumn
import com.thoughtworks.awayday.ingest.base.{PipelineTestBase, SparkTestingBase}
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

case class RawStudentCast(name: String, age: String, gpa: String)

class DataTypeCastStageTest extends FlatSpec with Matchers with SparkTestingBase with PipelineTestBase {

  "ReplaceCharTransformer" should "replace the find values with replace values correctly for all columns" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val rawData = List(
      RawStudentCast("a", "20", "4.0"),
      RawStudentCast("b", "25", "4.5"),
      RawStudentCast("c", "30", "4.8"),
      RawStudentCast("d", "35", "4.8")
    )

    val df = spark
      .sparkContext
      .parallelize(rawData)
      .toDF

    val dataCols = List(
      DataColumn("name", "string"),
      DataColumn("age", "long"),
      DataColumn("gpa", "double")
    )

    val expectedSchema = StructType(List(
      StructField("name", StringType),
      StructField("age", LongType),
      StructField("gpa", DoubleType)
    ))

    val stage = new DataTypeCastStage(dataCols)
    val returnDfWithErrors = stage(df)
    val (errors, returnDf) = returnDfWithErrors.run

    returnDf.count() shouldBe 4
    errors.count() shouldBe 0
    returnDf.as[RawStudentCast].collect().toSet shouldBe rawData.toSet
    returnDf.schema.fields shouldBe expectedSchema.fields

  }
}
