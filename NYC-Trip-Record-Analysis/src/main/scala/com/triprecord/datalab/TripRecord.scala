package com.triprecord.datalab

import com.triprecord.datalab.config.AppConfig
import com.triprecord.datalab.environment.SparkEnvironment
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import com.triprecord.datalab.util.FileUtil._
import com.triprecord.datalab.preprocess.PreProcess._
import com.triprecord.datalab.processes.ProcessPeakTime._
import com.triprecord.datalab.processes.ProcessAverages._
import com.triprecord.datalab.processes.ProcessAnamolies._
import com.triprecord.datalab.processes.ProcessRanking._
import com.triprecord.datalab.processes.ProcessCategorization._

import scala.util.{Failure, Success}

object TripRecord extends App {
  val log = Logger.getLogger(getClass.getName)
  log.info(s"Spark configuration: ${SparkEnvironment.spark.conf}")

  readFiles(AppConfig.srcPath, AppConfig.srcFileType) match {

    case Success(inputDf) =>
      log.info(s"Reading of files is successful")
      val preprocessedDf: DataFrame = inputDf.preprocess
      log.info(s"Trip data is preprocessed")
      val transformedDf: DataFrame = AppConfig.processName match {
        case "PeakTime" => findPeakTime(preprocessedDf)
        case "AvgAmountPaymentType" => findAvgAmtPerPaymentType(preprocessedDf)
        case "AnamoliesInTripDistance" => findAnamoliesTripDistance(preprocessedDf)
        case "AvgDistPerRatecodeid" => findAvgTripDistPerRatecodeid(preprocessedDf)
        case "VendorIdRanking" => rankOnFareAmount(preprocessedDf)
        case "CategoriseTrips" => categoriseOnAmount(preprocessedDf)
        case _ => throw new NotImplementedError("Given Process name is not implemented")
      }
      printRecord(transformedDf)
      val isWritten = writeFiles(transformedDf, AppConfig.destPath, AppConfig.destFileType, AppConfig.numPartitions)
      isWritten match {
        case Failure(ex) => log.error(s"Writing of files to ${AppConfig.destPath} failed. Reason: ${ex.getMessage}")
        case Success(_) => log.info(s"Writing of files to ${AppConfig.destPath} was successful")
      }

    case Failure(ex) => log.error(s"Reading of files from ${AppConfig.srcPath} failed. Reason: ${ex.getMessage} ")
  }

}
