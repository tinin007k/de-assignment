package com.triprecord.datalab.processes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ProcessRanking {
  def rankOnFareAmount(inputDf: DataFrame): DataFrame = {
    val dfWithDate = inputDf
      .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime"), "yyyy-MM-dd"))
      .select("VendorID", "Dropoff_date", "fare_amount")

    val rolledUpDF = dfWithDate.rollup("Dropoff_date", "VendorID")
      .agg(grouping_id().alias("gid"), sum("fare_amount"))
      .filter(expr("gid == 0"))
      .withColumnRenamed("Dropoff_date", "Date")
      .selectExpr("Date", "VendorID", "`sum(fare_amount)` as total_fare_amount")

    val rankDf = rolledUpDF.withColumn("rank",
      rank().over(Window.partitionBy("Date").orderBy(col("total_fare_amount").desc))
    ).orderBy("Date")

    rankDf
  }
}
