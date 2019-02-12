package com.thoughtworks.awayday.ingest

import java.util.UUID

import com.thoughtworks.awayday.config.ConfigModels.DataColumn
import com.thoughtworks.awayday.ingest.models.ErrorModels.DataError
import com.thoughtworks.awayday.ingest.stages.StageConstants.{RowKey, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

object UDFs {

  def replaceCharUdf(map: Map[String, String]) = udf((source: String) => {
    map.iterator.foldLeft(source) { case (src, (find, replace)) =>
      src.replace(find, replace)
    }
  })

  val generateUUID = udf(() => {
    s"${UUID.randomUUID().toString}_${System.nanoTime()}"
  })


  def validateRowUDF(columns: List[DataColumn], stage: String) = udf((row: Row) => {

    def validateDataTypes(row: Row, dataType: DataType, colConfig: DataColumn, index: Int): Option[DataError] = {
      if (!colConfig.nullable && row.isNullAt(index)) {
        Option(DataError(row.getString(0), stage, colConfig.name, "NULL_VALUE", s"Field is marked as not nullable but the value is null. Config was ${colConfig}", ErrorSeverity))
      }
      else {
        val tryResult =
          dataType match {
            case IntegerType => Try(row.getString(index).toInt)
            case LongType => Try(row.getString(index).toLong)
            case FloatType => Try(row.getString(index).toFloat)
            case DoubleType => Try(row.getString(index).toDouble)
            case BooleanType => Try(row.getString(index).toBoolean)
            case DateType =>
              val rawValue = row.getString(index)
              if (Option(rawValue).isEmpty) Success(null)
              else if (colConfig.format.isEmpty) sys.error(s"Date format not specified for column : ${colConfig.name}. Format value found was : ${colConfig}. Value of the dataType was ${row.getString(index)}.")
              else {
                val format = DateTimeFormat forPattern colConfig.format
                Try(format.parseDateTime(rawValue))
              }
            case t@_ => Failure(new Exception(s"validateDataTypes is not implemented for this datatype $t"))
          }

        tryResult match {
          case Success(_) => None
          case Failure(ex) =>
            val dataError = DataError(row.getAs[String](RowKey), stage, colConfig.name, row.getString(index), s"Value could not be converted to the target datatype '${colConfig.dType}'. Error: ${ex.getMessage}. Column config : $colConfig.", ErrorSeverity)
            Option(dataError)
        }
      }
    }

    val indexRange = 1 to columns.length
    val errorList = columns.tail.zip(indexRange).zip(columns).foldLeft(Seq[DataError]()) { case (errors, ((column, index), colConfig)) => CatalystSqlParser.parseDataType(column.dType) match {
      case StringType => errors
      case dataType@ (DoubleType | IntegerType | LongType | FloatType | BooleanType | DateType | TimestampType) =>
        val errorOpt = validateDataTypes(row, dataType, colConfig, index)
        errorOpt.map(err => errors :+ err).getOrElse(errors)
      case x@_ => println(s"Casting of type $x not implemented"); errors
    }
    }

    errorList

  })
}
