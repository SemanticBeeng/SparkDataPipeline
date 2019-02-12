package com.thoughtworks.awayday.ingest.stages

import com.thoughtworks.awayday.config.ConfigModels.DataColumn
import com.thoughtworks.awayday.ingest.base.{PipelineTestBase, SparkTestingBase}
import com.thoughtworks.awayday.ingest.models.ErrorModels.DataError
import com.thoughtworks.awayday.ingest.stages.StageConstants.RowKey
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class DataTypeValidatorTest extends FlatSpec with Matchers with SparkTestingBase with PipelineTestBase {

  "DataTypeValidatorTest" should "validate xxxxxxxx replace the find values with replace values correctly for all columns" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark
      .sparkContext
      .parallelize(List(
        ("r1", "a", "1", "0"),
        ("r2", "x", "t", "5"),
        ("r3", "y", "2", "1.0"),
        ("r4", "z", "v", "u")
      ))
      .toDF(RowKey, "name", "age", "gpa")

    val dataCols = List(
      DataColumn(RowKey, "string"),
      DataColumn("name", "string"),
      DataColumn("age", "long"),
      DataColumn("gpa", "double")
    )

    val dataTypeValidator = new DataTypeValidatorStage(dataCols)(spark)

    val returnDfWithErrors = dataTypeValidator(df)

    val (errors, returnDf) = returnDfWithErrors.run

    returnDf.createOrReplaceTempView("DataTypeCastDf")

    returnDf.show(false)
    returnDf.printSchema()

    errors.collect().foreach(println)
    returnDf.schema.fields.map(_.dataType) shouldBe Array(StringType, StringType, StringType, StringType)

    errors.count() shouldBe 3
    errors.collect().toSet shouldBe Set(
      DataError("r2", "DataTypeValidatorStage", "name", "t", "Value could not be converted to the target datatype 'string'. Error: For input string: \"t\". Column config : DataColumn(name,string,,true,false).", "ErrorSeverity", ""),
      DataError("r4", "DataTypeValidatorStage", "name", "v", "Value could not be converted to the target datatype 'string'. Error: For input string: \"v\". Column config : DataColumn(name,string,,true,false).", "ErrorSeverity", ""),
      DataError("r4", "DataTypeValidatorStage", "age", "u", "Value could not be converted to the target datatype 'long'. Error: For input string: \"u\". Column config : DataColumn(age,long,,true,false).", "ErrorSeverity", "")
    )


  }
}
