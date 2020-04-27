package com.aruba.arch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
trait CleaningTraits {
  def invalidLocationFilter(inputDf : DataFrame , spark :SparkSession) : DataFrame
  def detectDataLossFilter(inputDf : DataFrame, spark :SparkSession,yearOfProcessing:Int) : DataFrame
  def dropDataDuplicate(inputDf : DataFrame, spark :SparkSession) : DataFrame
}