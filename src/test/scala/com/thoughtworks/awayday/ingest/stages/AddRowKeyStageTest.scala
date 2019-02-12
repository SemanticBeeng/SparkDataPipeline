package com.thoughtworks.awayday.ingest.stages

import com.thoughtworks.awayday.ingest.base.{PipelineTestBase, SparkTestingBase}
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import com.thoughtworks.awayday.ingest.stages.StageConstants.RowKey

case class RawStudentAdd(name: String, age: String, gpa: String)

class AddRowKeyStageTest extends FlatSpec with Matchers with SparkTestingBase with PipelineTestBase {

  "ReplaceCharTransformer" should "replace the find values with replace values correctly for all columns" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark
      .sparkContext
      .parallelize(List(
        RawStudentAdd("a", "1", "0"),
        RawStudentAdd("b", "?", "0"),
        RawStudentAdd("c", "2", "?"),
        RawStudentAdd("d", "?", "?")
      ))
      .toDF

    val schema = StructType(List(
      StructField(RowKey, StringType),
      StructField("name", StringType),
      StructField("age", LongType),
      StructField("gpa", DoubleType)
    ))

    val stage = new AddRowKeyStage(schema)

    val returnDfWithErrors = stage(df)

    val (errors, returnDf) = returnDfWithErrors.run

    returnDf.show(false)

    returnDf.select(RowKey).dropDuplicates().count() shouldBe 4
    errors.count() shouldBe 0



  }
}
