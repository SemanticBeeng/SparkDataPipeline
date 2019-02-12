package com.thoughtworks.awayday.ingest

import cats.data.Writer
import cats.syntax.either._
import com.thoughtworks.awayday.PipelineUtils
import com.thoughtworks.awayday.config.ConfigModels
import com.thoughtworks.awayday.config.ConfigModels.Delimited
import com.thoughtworks.awayday.ingest.stages._
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object PipelineMain {

  def main(args: Array[String]): Unit = {
    val filePath = args(0)

    val sparkConf = buildSparkConf()

    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Boring Pipeline")
      .master("local[*]")
      .getOrCreate()

    /*  val awsAccessKey = System.getenv("AWS_ACCESS_KEY")
      val awsSecretAccessKey = System.getenv("AWS_SECRET_KEY")

      spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", awsAccessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", awsSecretAccessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")*/

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    //spark.sql("drop table if exists transactions")

    val fileName = StringUtils.substringAfterLast(filePath, "/")
    val dataSetE =
      for {
        ingestionConfig <- ConfigModels.getIngestionConfig(
          getClass.getResource("/datasets_main.conf"), "ingestionConfig")
        dataSetIn <- PipelineUtils.findDatasetFromFileName("evergreen", ingestionConfig) //Need to figure out how to remove the filename in StructuredStreaming
      } yield dataSetIn


    if (dataSetE.isLeft) {
      sys.error("No matching dataset configuration found for the given file. Please check the filePattern configuration for each dataset")
    }

    val dataSetConfig = dataSetE.right.get
    val schema = PipelineUtils.convertColumnsToStructTypeAndAddRowKey(dataSetConfig)
    //val rawFileSchema = PipelineUtils.convertColumnsToAllStringStructType(dataSetConfig)

    val sourceRawDf =
      spark
        .readStream
        .format("csv")
        .option("header", true)
        .option("delimiter", dataSetConfig.fileFormat.asInstanceOf[Delimited].delimiter) //FIXME There must be a better way to do this
        .load(filePath)

    import DataFrameOps._

    val DoubleCols = dataSetConfig.columns.filter(_.dType == "double").map(_.name)

    val pipelineStages = List(
      new AddRowKeyStage(schema),
      new ReplaceCharDataStage(DoubleColsReplaceMap, dataSetConfig.columns, DoubleCols),
      new DataTypeValidatorStage(dataSetConfig.columns),
      new DataTypeCastStage(dataSetConfig.columns)
    )
    //2.Set the DF into Monadic context
    //val initDf = sourceRawDf.pure[DataSetWithErrors]
    val initDf = Writer(DataFrameOps.emptyErrorStream(spark), sourceRawDf)

    val validRecords = pipelineStages.foldLeft(initDf) { case (dfWithErrors, stage) =>
      for {
        df <- dfWithErrors
        applied <- stage(df)
      } yield applied
    }

    //3.Chain them away
    /* val validRecords =
       for {
         initRecs <- initDf
         withRowKeys <- addRowKeyStage.apply(initRecs)
         _ <- writeToHBaseStage.apply(withRowKeys.persist())
         replaceCharsRecs <- replaceCharDataStage.apply(withRowKeys)
         validRecs <- dataTypeValidatorStage.apply(replaceCharsRecs)
         finalRecs <- dataTypeCastStage.apply(validRecs)
       } yield finalRecs*/

    //4.Retrieve the successful records and the cumulative errors. Profit :-)
    val (errors, processedDf) = validRecords.run

    //processedDf.union(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], processedDf.schema))

    /*processedDf.show(20, false)
    errors.show(20, false)*/

    val query = processedDf
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()


    val errorQuery = errors
      .writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()


    query.awaitTermination()

    /*processedDf
      .write
      .saveAsTable("transactions")*/


    /*processedDf.write
      .format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://sansiri-redshift.cnfisieh6wab.us-east-2.redshift.amazonaws.com:5439/dev?user=sansiri&password=Sansiri1")
      .option("dbtable", "transactions")
      .option("tempdir", s"s3a://sansiripoc/sansirtemp")
      //.option("tempdir", s"s3n://$awsAccessKey:$awsSecretAccessKey@sansiripoc/sansirtemp")
      .option("aws_iam_role", "arn:aws:iam::857423828993:role/RedshiftEc2S3Role")
      .mode(SaveMode.Overwrite)
      .save()*/

    /*processedDf.show()
    processedDf.printSchema()*/
    //Works
    /*processedDf.write
      .format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://sansiri-redshift.cnfisieh6wab.us-east-2.redshift.amazonaws.com:5439/dev?user=sansiri&password=Sansiri1")
      .option("dbtable", "transactions")
      //.option("tempdir", s"s3a://sansiripoc/sansirtemp")
      .option("tempdir", s"s3a://$awsAccessKey:$awsSecretAccessKey@sansiripoc/sansirtemp")
      .option("aws_iam_role", "arn:aws:iam::857423828993:role/RedshiftEc2S3Role")
      .mode(SaveMode.Overwrite)
      .save()*/

    /*   processedDf.write
         .format("parquet")
         .mode(SaveMode.Overwrite)
         .save(s"s3a://$awsAccessKey:$awsSecretAccessKey@sansiripoc/sansirtemp")*/


    /*val query = processedDf
      .write
      .format("console")
      .mode(SaveMode.Overwrite)
      .save()


    val errorQuery = errors
      .writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()*/


    //query.awaitTermination()
    //errorQuery.awaitTermination()

    /*//Batch
    processedDf.show(false)
    processedDf.printSchema()
    errors.foreach(println)*/


    /*val written = validRecords.written
    println("Printing Written")
    written.foreach(println)
    processedDf.show(false)*/


    //ML-Prepare
    //TrainModel.train(processedDf, FeatureCols, LabelCol)


    //Create Hive schema
    /*  sourceDF
        .write
        .format("parquet") //Needn't specify as the default is parqet gzip
        .saveAsTable("clickstream")


    */

  }

  /** ********************************************** THE FOLLOWING OUGHT TO BE DERIVED FROM CONFIGURATION ************************************/


  def buildSparkConf(): SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.default.parallelism", "1")
    .set("spark.sql.warehouse.dir", "/tmp/awaywarehose")
    .set("spark.kryoserializer.buffer.max", "1g")

  val DoubleColsReplaceMap = Map("?" -> "0.0")
  val SpecialCharMap = Map("\"" -> "")

  val HBaseTableName = "EverGreenTable"
  val HBaseColumnFamilyName = "allColsCF"
  val HBaseStagingDirectoryLocation = "/tmp/hbase_staging_dir"

}
