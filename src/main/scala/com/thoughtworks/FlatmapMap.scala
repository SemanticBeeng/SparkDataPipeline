package com.thoughtworks

import cats.data._
import cats.instances.list._

object FlatmapMap {

  /* def getCurrentTemperature(): Future[Double] = Future.successful(10.0)

   def getTomorrowsTempFromPredictionAPI(curr: Double): Future[Double] = Future.successful(20)

   def publishItInOurWebsite(pred: Double): Future[Double] = Future.successful(20)*/


  def getCurrentTemperatureW(): Writer[List[String], Double] = {
    Writer(List("Thermometer isn't broken yet"), 10.0)
  }

  def getTomorrowsTempFromPredictionAPIW(curr: Double): Writer[List[String], Double] = {
    Writer(List("Yay, the Prediction API works too"), 20.0)
  }

  def publishItInOurWebsiteW(pred: Double): Writer[List[String], Double] = {
    Writer(List("Published to our website"), 20.0)
  }


  /*def getCurrentTemperatureW(): WriterT[Future, List[String], Double] = {
    WriterT.putT(Future.successful(10.0))(List("Thermometer isn't broken yet"))
  }


  def getTomorrowsTempFromPredictionAPIW(curr: Double): WriterT[Future, List[String], Double] = {
    WriterT.putT(Future.successful(20.0))(List("Yay, the Prediction API works too"))
  }

  def publishItInOurWebsiteW(pred: Double): WriterT[Future, List[String], Double] = {
    WriterT.putT(Future.successful(20.0))(List("Published to our website"))
  }
*/
  def main(args: Array[String]): Unit = {

    /* val published1 = getCurrentTemperature()
       .flatMap { curr =>
         getTomorrowsTempFromPredictionAPI(curr)
           .flatMap(pred => publishItInOurWebsite(pred))
       }


     val published2: Future[Double] =
       for {
         curr <- getCurrentTemperature()
         pred <- getTomorrowsTempFromPredictionAPI(curr)
         pub <- publishItInOurWebsite(pred)
       } yield pub*/


    val publishedWriter: Writer[List[String], Double] =
      for {
        curr <- getCurrentTemperatureW()
        pred <- getTomorrowsTempFromPredictionAPIW(curr)
        pub <- publishItInOurWebsiteW(pred)
      } yield pub


    val (logs, value) = publishedWriter.run

    logs.foreach(println)
    println (value)


    /* val publishedWriter =
      for {
        curr <- getCurrentTemperatureW()
        pred <- getTomorrowsTempFromPredictionAPIW(curr)
        pub <- publishItInOurWebsiteW(pred)
      } yield pub


    val futureRun = publishedWriter.run

    val (logs, value) = Await.result(futureRun, 2 seconds)

    println(logs.mkString("\n"))
    println(s"\nValue is $value")*/


    //println (logs.mkString(","))


  }


}
