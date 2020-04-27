package com.aruba.utils

import com.aruba.arch.CleaningTraits
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object CleaningUtils extends CleaningTraits {
  val log = Logger.getLogger(getClass.getName)
  def dataPreprocessingUtility(inputDf: DataFrame, spark: SparkSession, yearOfProcessing: Int): DataFrame = {
    try {
      log.info("DetectData Loss Filter Executing")
      val firstLevelCleaning = detectDataLossFilter(inputDf, spark, yearOfProcessing)
      log.info("Invalid Location Filtering Executing")
      val secondLevelCleaning = invalidLocationFilter(firstLevelCleaning, spark)
      log.info("Dropping duplicate records Executing")
      val cleanedInputDataDf = dropDataDuplicate(secondLevelCleaning, spark)
      cleanedInputDataDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing Data Preprocessing Utility")
        System.exit(1)
        null
    }

  }

  override def invalidLocationFilter(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      val outputDf = inputDf.filter(length(trim($"PULocationID")) > 0 && length(trim($"DOLocationID")) > 0 && $"trip_distance" > 0.00)
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing Data cleaning : invalidLocationFilter")
        System.exit(1)
        null
    }
  }

  override def detectDataLossFilter(inputDf: DataFrame, spark: SparkSession, yearOfProcessing: Int): DataFrame = {
    import spark.implicits._
    try {
      val filterBasedOnVendorIDdf = inputDf.filter(length(trim($"VendorID")) > 0).filter(row => row.getAs[String]("VendorID").matches("""\d+"""))
      val timeStampValidationDf = filterBasedOnVendorIDdf.withColumn("tpep_pickup_datetime", expr("to_timestamp(tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss')"))
        .withColumn("tpep_dropoff_datetime", expr("to_timestamp(tpep_dropoff_datetime,'yyyy-MM-dd HH:mm:ss')"))
        .filter($"tpep_dropoff_datetime" > $"tpep_pickup_datetime" && year($"tpep_dropoff_datetime") === yearOfProcessing && year($"tpep_pickup_datetime") === yearOfProcessing)
        .filter($"passenger_count" > 0)
      timeStampValidationDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing Data cleaning : detectDataLossFilter")
        System.exit(1)
        null
    }
  }

  override def dropDataDuplicate(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    return inputDf.dropDuplicates()
  }

}