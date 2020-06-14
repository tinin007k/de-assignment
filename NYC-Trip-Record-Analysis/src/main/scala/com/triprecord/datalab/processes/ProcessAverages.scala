package com.triprecord.datalab.processes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProcessAverages {

  def findAvgAmtPerPaymentType(inputDf: DataFrame): DataFrame = {
    val resultDf = inputDf.groupBy("payment_type")
      .agg(avg("total_amount").alias("Average_amount"))
    resultDf
  }

  def findAvgTripDistPerRatecodeid(inputDf: DataFrame): DataFrame = {
    val resultDf = inputDf.groupBy("RatecodeID")
      .agg(avg("trip_distance").alias("avg_trip_distance"))
    resultDf
  }
}
