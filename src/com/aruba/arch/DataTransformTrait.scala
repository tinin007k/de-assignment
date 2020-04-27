package com.aruba.arch


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
trait DataTransformTrait {
  def findDailyPeakHourDf(inputDf : DataFrame , spark :SparkSession) : DataFrame
  def avgAmountPerPaymentTypeDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
  def anomaliesAvgTripDistanceVendorwiseDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
  def anomalousTripVendorwiseDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
  def avgDistancePerRateCodeIdDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
  def rankVendorIdByFareAmountDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
  def categoryTripByTotalAmountDf(inputDf : DataFrame, spark :SparkSession) : DataFrame
}